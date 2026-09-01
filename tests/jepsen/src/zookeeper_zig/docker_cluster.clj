(ns zookeeper-zig.docker-cluster
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]])
  (:import (java.io File)
           (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.net InetSocketAddress Socket)
           (java.nio.file Files Path)
           (java.time Instant)
           (java.util.concurrent TimeUnit)))

(def command-timeout-seconds 60)
(def startup-timeout-ms 30000)
(def client-port 2181)
(def raft-port 7000)
(def partition-chain "ZKJ_PARTITION")

(def ^:dynamic *docker-command-log*
  (System/getenv "ZOOKEEPER_ZIG_DOCKER_LOG"))

(def ^:private docker-command-log-lock (Object.))

(defn- command-failure-message
  [message logged?]
  (cond
    logged?
    (str message "; details logged to " *docker-command-log*)

    (seq *docker-command-log*)
    (str message "; unable to write details to " *docker-command-log*)

    :else
    message))

(defn- log-command-failure!
  [{:keys [command exit output]}]
  (if-not (seq *docker-command-log*)
    false
    (try
      (let [log-file (File. *docker-command-log*)
            parent (.getParentFile log-file)]
        (when parent
          (.mkdirs parent))
        (locking docker-command-log-lock
          (spit log-file
                (str "timestamp: " (Instant/now) "\n"
                     "command: " (pr-str command) "\n"
                     "exit: " exit "\n"
                     "output:\n"
                     (if (str/blank? output) "(empty)" output)
                     "\n\n")
                :append true))
        true)
      (catch Throwable _
        false))))

(defn- run-result
  [arguments]
  (let [command (into ["docker"] arguments)
        process (-> (ProcessBuilder. ^java.util.List command)
                    (.redirectErrorStream true)
                    (.start))
        output (future (slurp (.getInputStream process)))]
    (when-not (.waitFor process command-timeout-seconds TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 TimeUnit/SECONDS)
      (let [result {:command command
                    :exit :timeout
                    :output (str/trim @output)}
            logged? (log-command-failure! result)]
        (throw (ex-info (command-failure-message "Docker command timed out"
                                                 logged?)
                        (dissoc result :output)))))
    {:exit (.exitValue process)
     :output (str/trim @output)
     :command command}))

(defn- run!
  [& arguments]
  (let [{:keys [exit output command] :as result} (run-result arguments)]
    (when-not (zero? exit)
      (let [logged? (log-command-failure! result)]
        (throw (ex-info (command-failure-message "Docker command failed" logged?)
                        {:command command
                         :exit exit
                         :log-file (when logged? *docker-command-log*)}))))
    output))

(defn- run-ok?
  [& arguments]
  (try
    (zero? (:exit (run-result arguments)))
    (catch Throwable _
      false)))

(defn- try-run!
  [& arguments]
  (try
    (apply run! arguments)
    true
    (catch Throwable error
      (warn "Ignoring Docker cleanup failure" (ex-message error))
      false)))

(defn- container-name
  [cluster node]
  (get-in cluster [:container-names node]))

(defn- node-address
  [cluster node]
  (get-in @(:state cluster) [:docker :addresses node]))

(defn- ipv4-number
  [address]
  (reduce (fn [result octet]
            (+ (bit-shift-left result 8) (Long/parseLong octet)))
          0
          (str/split address #"\.")))

(defn- ipv4-string
  [address]
  (str (bit-and 255 (unsigned-bit-shift-right address 24)) "."
       (bit-and 255 (unsigned-bit-shift-right address 16)) "."
       (bit-and 255 (unsigned-bit-shift-right address 8)) "."
       (bit-and 255 address)))

(defn subnet-address
  [cidr offset]
  (let [[address prefix-text] (str/split cidr #"/")
        prefix (Long/parseLong prefix-text)
        host-bits (- 32 prefix)
        capacity (bit-shift-left 1 host-bits)
        mask (if (zero? prefix)
               0
               (bit-and 0xffffffff
                        (bit-shift-left 0xffffffff host-bits)))
        network (bit-and (ipv4-number address) mask)]
    (when-not (< 1 offset (dec capacity))
      (throw (ex-info "Docker network subnet is too small"
                      {:cidr cidr :offset offset})))
    (ipv4-string (+ network offset))))

(defn- container-command
  [cluster node addresses]
  (let [{:keys [id]} (get-in cluster [:configs node])
        peers (mapcat (fn [peer]
                        ["--peer"
                         (str (get-in cluster [:configs peer :id]) "="
                              (get addresses peer) ":" raft-port)])
                      (:nodes cluster))]
    (into ["/opt/zookeeper-zig/zookeeper-quorum-server"
           "--node-id" (str id)
           "--cluster-id" (:cluster-id cluster)
           "--client-listen" (str "0.0.0.0:" client-port)
           "--raft-listen" (str "0.0.0.0:" raft-port)
           "--data-dir" "/var/lib/zookeeper-zig"]
          peers)))

(defn- controller-container
  []
  (let [hostname (System/getenv "HOSTNAME")]
    (when (seq hostname)
      (let [{:keys [exit]} (run-result ["inspect" hostname])]
        (when (zero? exit)
          hostname)))))

(defn- attach-controller!
  [network]
  (when-let [controller (controller-container)]
    (let [mode (run! "inspect" "--format" "{{.HostConfig.NetworkMode}}"
                     controller)]
      (when-not (= "host" mode)
        (let [address (run! "inspect" "--format"
                            (str "{{with index .NetworkSettings.Networks \""
                                 network "\"}}{{.IPAddress}}{{end}}")
                            controller)]
          (when (str/blank? address)
            (run! "network" "connect" network controller)
            controller))))))

(defn- cleanup-resources!
  [containers network controller strict?]
  (let [execute (if strict?
                  (fn [& arguments] (apply run! arguments))
                  (fn [& arguments] (apply try-run! arguments)))]
    (doseq [container containers]
      (execute "rm" "--force" container))
    (when controller
      (execute "network" "disconnect" network controller))
    (when network
      (execute "network" "rm" network))))

(defn- ensure-cluster!
  [cluster]
  (locking (:state cluster)
    (when-not (get-in @(:state cluster) [:docker :initialized?])
      (let [network (:network-name cluster)
            containers (mapv #(container-name cluster %) (:nodes cluster))
            image (:node-image cluster)
            label (str "zookeeper-zig.jepsen.run=" (:run-id cluster))
            created (atom [])
            network-created? (atom false)
            controller (atom nil)]
        (try
          (run! "image" "inspect" image)
          (run! "network" "create" "--label" label network)
          (reset! network-created? true)
          (reset! controller (attach-controller! network))
          (let [cidr (run! "network" "inspect" "--format"
                           "{{(index .IPAM.Config 0).Subnet}}" network)
                addresses (into {}
                                (map-indexed
                                 (fn [index node]
                                   [node (subnet-address cidr (+ 10 index))])
                                 (:nodes cluster)))]
            (doseq [node (:nodes cluster)]
              (let [container (container-name cluster node)
                    address (get addresses node)
                    arguments (into ["create"
                                     "--name" container
                                     "--hostname" (name node)
                                     "--network" network
                                     "--ip" address
                                     "--cap-add" "NET_ADMIN"
                                     "--security-opt" "seccomp=unconfined"
                                     "--init"
                                     "--label" label
                                     image]
                                    (container-command cluster node addresses))]
                (apply run! arguments)
                (swap! created conj container)
                (run! "cp" (:binary cluster)
                      (str container ":/opt/zookeeper-zig/zookeeper-quorum-server"))))
            (swap! (:state cluster)
                   assoc :docker {:initialized? true
                                  :network network
                                  :controller @controller
                                  :addresses addresses}
                   :nodes {})
            (info "Created Docker ZooKeeper cluster" network addresses))
          (catch Throwable error
            (cleanup-resources! @created
                                (when @network-created? network)
                                @controller
                                false)
            (throw error)))))))

(defn connect-string
  [cluster]
  (ensure-cluster! cluster)
  (->> (:nodes cluster)
       (map #(str (node-address cluster %) ":" client-port))
       (str/join ",")))

(defn- running?
  [cluster node]
  (and (true? (get-in @(:state cluster) [:nodes node :running?]))
       (= "true"
          (run! "inspect" "--format" "{{.State.Running}}"
                (container-name cluster node)))))

(defn- await-port!
  [cluster node]
  (let [address (node-address cluster node)
        deadline (+ (System/nanoTime)
                    (.toNanos TimeUnit/MILLISECONDS startup-timeout-ms))]
    (loop []
      (if (try
            (with-open [socket (Socket.)]
              (.connect socket (InetSocketAddress. address client-port) 200)
              true)
            (catch java.io.IOException _
              false))
        true
        (let [docker-running? (= "true"
                                 (run! "inspect" "--format"
                                       "{{.State.Running}}"
                                       (container-name cluster node)))]
          (when-not docker-running?
            (throw (ex-info "Docker ZooKeeper node exited during startup"
                            {:node node})))
          (if (< (System/nanoTime) deadline)
            (do
              (Thread/sleep 50)
              (recur))
            (throw (ex-info "Docker ZooKeeper node did not listen in time"
                            {:node node :address address}))))))))

(defn start-node!
  [cluster node]
  (ensure-cluster! cluster)
  (locking (:state cluster)
    (if (running? cluster node)
      :already-running
      (do
        (run! "start" (container-name cluster node))
        (swap! (:state cluster) assoc-in [:nodes node :running?] true)
        (try
          (await-port! cluster node)
          (info "Started Docker ZooKeeper node" node (node-address cluster node))
          :started
          (catch Throwable error
            (try-run! "kill" (container-name cluster node))
            (swap! (:state cluster) assoc-in [:nodes node :running?] false)
            (throw error)))))))

(defn- stop-container!
  [cluster node force?]
  (locking (:state cluster)
    (if-not (running? cluster node)
      :already-stopped
      (do
        (if force?
          (run! "kill" (container-name cluster node))
          (run! "stop" "--time" "10" (container-name cluster node)))
        (swap! (:state cluster) assoc-in [:nodes node :running?] false)
        (info (if force? "Killed" "Stopped") "Docker ZooKeeper node" node)
        (if force? :killed :stopped)))))

(defn stop-node!
  [cluster node]
  (stop-container! cluster node false))

(defn kill-node!
  [cluster node]
  (stop-container! cluster node true))

(defn pause-node!
  [cluster node]
  (locking (:state cluster)
    (cond
      (not (running? cluster node)) :already-stopped
      (get-in @(:state cluster) [:nodes node :paused?]) :already-paused
      :else
      (do
        (run! "pause" (container-name cluster node))
        (swap! (:state cluster) assoc-in [:nodes node :paused?] true)
        :paused))))

(defn resume-node!
  [cluster node]
  (locking (:state cluster)
    (cond
      (not (running? cluster node)) :already-stopped
      (not (get-in @(:state cluster) [:nodes node :paused?])) :already-running
      :else
      (do
        (run! "unpause" (container-name cluster node))
        (swap! (:state cluster) assoc-in [:nodes node :paused?] false)
        :resumed))))

(defn running-nodes
  [cluster]
  (locking (:state cluster)
    (->> (:nodes cluster)
         (filter #(running? cluster %))
         vec)))

(defn- reset-partition-container!
  [container]
  (run-ok? "exec" container "iptables" "-w" "5" "-D" "INPUT"
           "-j" partition-chain)
  (run-ok? "exec" container "iptables" "-w" "5" "-D" "OUTPUT"
           "-j" partition-chain)
  (run-ok? "exec" container "iptables" "-w" "5" "-F" partition-chain)
  (run-ok? "exec" container "iptables" "-w" "5" "-X" partition-chain)
  (let [rules (run! "exec" container "iptables" "-w" "5" "-S")]
    (when (str/includes? rules partition-chain)
      (throw (ex-info "Failed to remove Docker partition rules"
                      {:container container :rules rules})))))

(defn- dropped-packets
  [container]
  (let [output (run! "exec" container "iptables" "-w" "5" "-L"
                     partition-chain "-v" "-n" "-x")]
    (->> (str/split-lines output)
         (keep (fn [line]
                 (when-let [[_ packets]
                            (re-find #"^\s*(\d+)\s+\d+\s+DROP\s" line)]
                   (Long/parseLong packets))))
         (reduce + 0))))

(defn heal-partition!
  [cluster]
  (ensure-cluster! cluster)
  (let [partitioned (get-in @(:state cluster) [:docker :partition])
        nodes (running-nodes cluster)
        stopped (remove (set nodes) (:nodes cluster))]
    (when (seq stopped)
      (throw (ex-info "Cannot heal partition while Docker nodes are stopped"
                      {:partitioned partitioned :stopped (vec stopped)})))
    (let [evidence (when partitioned
                     (into {}
                           (map (fn [node]
                                  [node (dropped-packets
                                         (container-name cluster node))])
                                nodes)))]
      (doseq [node nodes]
        (reset-partition-container! (container-name cluster node)))
      (swap! (:state cluster) update :docker dissoc :partition)
      (when partitioned
        (info "Healed Docker partition" partitioned "dropped packets" evidence)
        (when (zero? (reduce + (vals evidence)))
          (throw (ex-info "Docker partition did not drop peer traffic"
                          {:partitioned partitioned :evidence evidence}))))
      :healed)))

(defn- install-partition-rules!
  [cluster node blocked-addresses]
  (let [container (container-name cluster node)]
    (reset-partition-container! container)
    (run! "exec" container "iptables" "-w" "5" "-N" partition-chain)
    (doseq [address blocked-addresses]
      (run! "exec" container "iptables" "-w" "5" "-A" partition-chain
            "-s" address "-j" "DROP")
      (run! "exec" container "iptables" "-w" "5" "-A" partition-chain
            "-d" address "-j" "DROP"))
    (run! "exec" container "iptables" "-w" "5" "-I" "INPUT" "1"
          "-j" partition-chain)
    (run! "exec" container "iptables" "-w" "5" "-I" "OUTPUT" "1"
          "-j" partition-chain)))

(defn partition-node!
  [cluster isolated]
  (ensure-cluster! cluster)
  (heal-partition! cluster)
  (let [others (remove #{isolated} (:nodes cluster))
        isolated-address (node-address cluster isolated)]
    (try
      (install-partition-rules! cluster isolated
                                (map #(node-address cluster %) others))
      (doseq [node others]
        (install-partition-rules! cluster node [isolated-address]))
      (swap! (:state cluster) assoc-in [:docker :partition] isolated)
      (info "Partitioned Docker ZooKeeper node" isolated)
      :partitioned
      (catch Throwable error
        (heal-partition! cluster)
        (throw error)))))

(defn- preserve-container-artifacts!
  [cluster node]
  (let [container (container-name cluster node)
        data-dir (get-in cluster [:configs node :data-dir])
        ^File log-file (get-in cluster [:configs node :log-file])]
    (spit log-file (str "\n--- docker logs for " node " ---\n") :append true)
    (let [process (-> (ProcessBuilder. ^java.util.List
                                       ["docker" "logs" container])
                      (.redirectErrorStream true)
                      (.redirectOutput (ProcessBuilder$Redirect/appendTo log-file))
                      (.start))]
      (when-not (.waitFor process command-timeout-seconds TimeUnit/SECONDS)
        (.destroyForcibly process)
        (throw (ex-info "Timed out preserving Docker node logs"
                        {:node node :container container})))
      (when-not (zero? (.exitValue process))
        (throw (ex-info "Failed to preserve Docker node logs"
                        {:node node
                         :container container
                         :exit (.exitValue process)}))))
    (run! "cp" (str container ":/var/lib/zookeeper-zig") (str data-dir))))

(defn cleanup!
  [cluster]
  (locking (:state cluster)
    (when (get-in @(:state cluster) [:docker :initialized?])
      (doseq [node (:nodes cluster)]
        (preserve-container-artifacts! cluster node))
      (let [{:keys [network controller]} (:docker @(:state cluster))]
        (cleanup-resources! (mapv #(container-name cluster %) (:nodes cluster))
                            network controller true))
      (swap! (:state cluster) assoc :docker {:initialized? false} :nodes {})
      (info "Removed Docker ZooKeeper cluster" (:network-name cluster)))))
