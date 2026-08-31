(ns zookeeper-zig.cluster
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]])
  (:import (java.io File)
           (java.net InetSocketAddress ServerSocket Socket)
           (java.nio.file Files Path Paths)
           (java.util UUID)
           (java.util.concurrent TimeUnit)
           (java.lang ProcessBuilder ProcessBuilder$Redirect)))

(def startup-timeout-ms 30000)

(defn- reserve-ports
  [count]
  (let [sockets (vec (repeatedly count #(ServerSocket. 0)))]
    (try
      (mapv #(.getLocalPort ^ServerSocket %) sockets)
      (finally
        (doseq [socket sockets]
          (.close ^ServerSocket socket))))))

(defn- await-port!
  [process port]
  (let [deadline (+ (System/nanoTime)
                    (.toNanos TimeUnit/MILLISECONDS startup-timeout-ms))]
    (loop []
      (when-not (.isAlive ^Process process)
        (throw (ex-info "ZooKeeper Zig node exited during startup"
                        {:port port
                         :exit (.exitValue ^Process process)})))
      (if (try
            (with-open [socket (Socket.)]
              (.connect socket (InetSocketAddress. "127.0.0.1" port) 200)
              true)
            (catch java.io.IOException _
              false))
        true
        (if (< (System/nanoTime) deadline)
          (do
            (Thread/sleep 50)
            (recur))
          (throw (ex-info "ZooKeeper Zig node did not listen in time"
                          {:port port})))))))

(defn cluster
  [binary nodes run-root]
  (let [node-count (count nodes)
        ports (reserve-ports (* 2 node-count))
        root (.resolve (Paths/get run-root (make-array String 0))
                       (str (UUID/randomUUID)))
        configs (into {}
                      (map-indexed
                       (fn [index node]
                         [node {:id (inc index)
                                :client-port (ports index)
                                :raft-port (ports (+ node-count index))
                                :data-dir (.resolve root (str "node-" (inc index)))
                                :log-file (.toFile
                                           (.resolve root
                                                     (str "node-" (inc index)
                                                          ".log")))}])
                       nodes))]
    (Files/createDirectories root (make-array java.nio.file.attribute.FileAttribute 0))
    {:binary binary
     :cluster-id (str (UUID/randomUUID))
     :nodes (vec nodes)
     :configs configs
     :root root
     :state (atom {})}))

(defn connect-string
  [cluster]
  (->> (:nodes cluster)
       (map #(str "127.0.0.1:" (get-in cluster [:configs % :client-port])))
       (str/join ",")))

(defn- command
  [cluster node]
  (let [{:keys [id client-port raft-port data-dir]} (get-in cluster [:configs node])
        peers (mapcat (fn [peer]
                        (let [{peer-id :id peer-port :raft-port}
                              (get-in cluster [:configs peer])]
                          ["--peer" (str peer-id "=127.0.0.1:" peer-port)]))
                      (:nodes cluster))]
    (into [(:binary cluster)
           "--node-id" (str id)
           "--cluster-id" (:cluster-id cluster)
           "--client-listen" (str "127.0.0.1:" client-port)
           "--raft-listen" (str "127.0.0.1:" raft-port)
           "--data-dir" (str data-dir)]
          peers)))

(defn start-node!
  [cluster node]
  (locking (:state cluster)
    (let [{:keys [client-port data-dir log-file]} (get-in cluster [:configs node])
          current (get-in @(:state cluster) [node :process])]
      (if (and current (.isAlive ^Process current))
        :already-running
        (do
          (Files/createDirectories ^Path data-dir
                                   (make-array java.nio.file.attribute.FileAttribute 0))
          (spit log-file (str "\n--- starting " node " ---\n") :append true)
          (let [builder (ProcessBuilder. ^java.util.List (command cluster node))
                process (-> builder
                            (.redirectErrorStream true)
                            (.redirectOutput (ProcessBuilder$Redirect/appendTo
                                              ^File log-file))
                            (.start))]
            (swap! (:state cluster) assoc node {:process process})
            (try
              (await-port! process client-port)
              (info "Started ZooKeeper Zig node" node "on" client-port)
              :started
              (catch Throwable error
                (.destroyForcibly ^Process process)
                (swap! (:state cluster) dissoc node)
                (throw error)))))))))

(defn- terminate-node!
  [cluster node force?]
  (locking (:state cluster)
    (if-let [process (get-in @(:state cluster) [node :process])]
      (do
        (swap! (:state cluster) dissoc node)
        (if force?
          (.destroyForcibly ^Process process)
          (.destroy ^Process process))
        (when-not (.waitFor ^Process process 10 TimeUnit/SECONDS)
          (.destroyForcibly ^Process process)
          (when-not (.waitFor ^Process process 10 TimeUnit/SECONDS)
            (throw (ex-info "Failed to stop ZooKeeper Zig node" {:node node}))))
        (info (if force? "Killed" "Stopped") "ZooKeeper Zig node" node)
        (if force? :killed :stopped))
      :already-stopped)))

(defn stop-node!
  [cluster node]
  (terminate-node! cluster node false))

(defn kill-node!
  [cluster node]
  (terminate-node! cluster node true))

(defn running-nodes
  [cluster]
  (locking (:state cluster)
    (->> (:nodes cluster)
         (filter (fn [node]
                   (when-let [process (get-in @(:state cluster) [node :process])]
                     (.isAlive ^Process process))))
         vec)))
