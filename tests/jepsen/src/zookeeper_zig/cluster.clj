(ns zookeeper-zig.cluster
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [zookeeper-zig.docker-cluster :as docker-cluster])
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
  ([binary nodes run-root]
   (cluster binary nodes run-root :process))
  ([binary nodes run-root mode]
   (let [node-count (count nodes)
         ports (reserve-ports (* 2 node-count))
         cluster-id (str (UUID/randomUUID))
         suffix (subs (str/replace cluster-id "-" "") 0 12)
         root (.resolve (Paths/get run-root (make-array String 0))
                        cluster-id)
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
                        nodes))
         container-names (into {}
                               (map (fn [node]
                                      [node (str "zkj-" suffix "-"
                                                 (str/replace (str node)
                                                              #"[^A-Za-z0-9_.-]"
                                                              "-"))])
                                    nodes))]
     (Files/createDirectories root (make-array java.nio.file.attribute.FileAttribute 0))
     {:binary binary
      :cluster-id cluster-id
      :mode mode
      :node-image (or (System/getenv "ZOOKEEPER_ZIG_NODE_IMAGE")
                      "zookeeper-zig-jepsen-node:0.1")
      :run-id (or (System/getenv "ZOOKEEPER_ZIG_RUN_ID") suffix)
      :network-name (str "zkj-" suffix)
      :container-names container-names
      :nodes (vec nodes)
      :configs configs
      :root root
      :state (atom {})})))

(defn docker-mode?
  [cluster]
  (= :docker (:mode cluster)))

(defn connect-string
  [cluster]
  (if (docker-mode? cluster)
    (docker-cluster/connect-string cluster)
    (->> (:nodes cluster)
         (map #(str "127.0.0.1:" (get-in cluster [:configs % :client-port])))
         (str/join ","))))

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

(defn- start-process-node!
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

(defn- signal-process!
  [^Process process signal]
  (let [command ["kill" (str "-" signal) (str (.pid process))]
        signal-process (.start (ProcessBuilder. ^java.util.List command))]
    (when-not (.waitFor signal-process 10 TimeUnit/SECONDS)
      (.destroyForcibly signal-process)
      (throw (ex-info "Timed out signaling ZooKeeper Zig node"
                      {:pid (.pid process) :signal signal})))
    (when-not (zero? (.exitValue signal-process))
      (throw (ex-info "Failed to signal ZooKeeper Zig node"
                      {:pid (.pid process)
                       :signal signal
                       :exit (.exitValue signal-process)})))))

(defn- pause-process-node!
  [cluster node]
  (locking (:state cluster)
    (if-let [{:keys [process paused?]} (get @(:state cluster) node)]
      (cond
        paused? :already-paused
        (not (.isAlive ^Process process)) :already-stopped
        :else
        (do
          (signal-process! process "STOP")
          (swap! (:state cluster) assoc-in [node :paused?] true)
          (info "Paused ZooKeeper Zig node" node)
          :paused))
      :already-stopped)))

(defn- resume-process-node!
  [cluster node]
  (locking (:state cluster)
    (if-let [{:keys [process paused?]} (get @(:state cluster) node)]
      (if paused?
        (do
          (signal-process! process "CONT")
          (swap! (:state cluster) assoc-in [node :paused?] false)
          (info "Resumed ZooKeeper Zig node" node)
          :resumed)
        :already-running)
      :already-stopped)))

(defn- terminate-process-node!
  [cluster node force?]
  (locking (:state cluster)
    (if-let [{:keys [process paused?]} (get @(:state cluster) node)]
      (do
        (when paused?
          (signal-process! process "CONT"))
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

(defn- running-process-nodes
  [cluster]
  (locking (:state cluster)
    (->> (:nodes cluster)
         (filter (fn [node]
                   (when-let [process (get-in @(:state cluster) [node :process])]
                     (.isAlive ^Process process))))
         vec)))

(defn start-node!
  [cluster node]
  (if (docker-mode? cluster)
    (docker-cluster/start-node! cluster node)
    (start-process-node! cluster node)))

(defn stop-node!
  [cluster node]
  (if (docker-mode? cluster)
    (docker-cluster/stop-node! cluster node)
    (terminate-process-node! cluster node false)))

(defn kill-node!
  [cluster node]
  (if (docker-mode? cluster)
    (docker-cluster/kill-node! cluster node)
    (terminate-process-node! cluster node true)))

(defn pause-node!
  [cluster node]
  (if (docker-mode? cluster)
    (docker-cluster/pause-node! cluster node)
    (pause-process-node! cluster node)))

(defn resume-node!
  [cluster node]
  (if (docker-mode? cluster)
    (docker-cluster/resume-node! cluster node)
    (resume-process-node! cluster node)))

(defn running-nodes
  [cluster]
  (if (docker-mode? cluster)
    (docker-cluster/running-nodes cluster)
    (running-process-nodes cluster)))

(defn partition-node!
  [cluster node]
  (when-not (docker-mode? cluster)
    (throw (ex-info "Network partitions require Docker nodes" {:node node})))
  (docker-cluster/partition-node! cluster node))

(defn heal-partition!
  [cluster]
  (when (docker-mode? cluster)
    (docker-cluster/heal-partition! cluster)))

(defn teardown-node!
  [cluster node]
  (let [result (stop-node! cluster node)]
    (when (docker-mode? cluster)
      (let [finished? (locking (:state cluster)
                        (swap! (:state cluster) update :teardown-nodes
                               (fnil conj #{}) node)
                        (= (set (:nodes cluster))
                           (:teardown-nodes @(:state cluster))))]
        (when finished?
          (docker-cluster/cleanup! cluster))))
    result))
