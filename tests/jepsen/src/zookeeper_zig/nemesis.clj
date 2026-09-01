(ns zookeeper-zig.nemesis
  (:require [jepsen.nemesis :as nemesis]
            [zookeeper-zig.cluster :as cluster]))

(defrecord KillOneNemesis [cluster disrupted]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (locking disrupted
      (case (:f op)
        :start
        (if @disrupted
          (assoc op :type :info :value {:already-killed @disrupted})
          (let [candidates (cluster/running-nodes cluster)
                node (when (seq candidates) (rand-nth candidates))]
            (if node
              (do
                (cluster/kill-node! cluster node)
                (reset! disrupted node)
                (assoc op :type :info :value {:killed node}))
              (assoc op :type :info :value :no-running-node))))

        :stop
        (if-let [node @disrupted]
          (do
            (cluster/start-node! cluster node)
            (reset! disrupted nil)
            (assoc op :type :info :value {:restarted node}))
          (assoc op :type :info :value :nothing-to-restart)))))

  (teardown! [this _]
    (locking disrupted
      (when-let [node @disrupted]
        (cluster/start-node! cluster node)
        (reset! disrupted nil)))
    this)

  nemesis/Reflection
  (fs [_]
    #{:start :stop}))

(defrecord PauseOneNemesis [cluster disrupted]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (locking disrupted
      (case (:f op)
        :start
        (if @disrupted
          (assoc op :type :info :value {:already-paused @disrupted})
          (let [candidates (cluster/running-nodes cluster)
                node (when (seq candidates) (rand-nth candidates))]
            (if node
              (do
                (cluster/pause-node! cluster node)
                (reset! disrupted node)
                (assoc op :type :info :value {:paused node}))
              (assoc op :type :info :value :no-running-node))))

        :stop
        (if-let [node @disrupted]
          (do
            (cluster/resume-node! cluster node)
            (reset! disrupted nil)
            (assoc op :type :info :value {:resumed node}))
          (assoc op :type :info :value :nothing-to-resume)))))

  (teardown! [this _]
    (locking disrupted
      (when-let [node @disrupted]
        (cluster/resume-node! cluster node)
        (reset! disrupted nil)))
    this)

  nemesis/Reflection
  (fs [_]
    #{:start :stop}))

(defrecord KillAllNemesis [cluster disrupted]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (locking disrupted
      (case (:f op)
        :start
        (if @disrupted
          (assoc op :type :info :value :already-killed)
          (let [nodes (cluster/running-nodes cluster)]
            (doseq [node nodes]
              (cluster/kill-node! cluster node))
            (reset! disrupted nodes)
            (assoc op :type :info :value {:killed nodes})))

        :stop
        (if-let [nodes @disrupted]
          (do
            (doseq [node nodes]
              (cluster/start-node! cluster node))
            (reset! disrupted nil)
            (assoc op :type :info :value {:restarted nodes}))
          (assoc op :type :info :value :nothing-to-restart)))))

  (teardown! [this _]
    (locking disrupted
      (when-let [nodes @disrupted]
        (doseq [node nodes]
          (cluster/start-node! cluster node))
        (reset! disrupted nil)))
    this)

  nemesis/Reflection
  (fs [_]
    #{:start :stop}))

(defrecord PauseAllNemesis [cluster disrupted]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (locking disrupted
      (case (:f op)
        :start
        (if @disrupted
          (assoc op :type :info :value :already-paused)
          (let [nodes (cluster/running-nodes cluster)]
            (doseq [node nodes]
              (cluster/pause-node! cluster node))
            (reset! disrupted nodes)
            (assoc op :type :info :value {:paused nodes})))

        :stop
        (if-let [nodes @disrupted]
          (do
            (doseq [node nodes]
              (cluster/resume-node! cluster node))
            (reset! disrupted nil)
            (assoc op :type :info :value {:resumed nodes}))
          (assoc op :type :info :value :nothing-to-resume)))))

  (teardown! [this _]
    (locking disrupted
      (when-let [nodes @disrupted]
        (doseq [node nodes]
          (cluster/resume-node! cluster node))
        (reset! disrupted nil)))
    this)

  nemesis/Reflection
  (fs [_]
    #{:start :stop}))

(defrecord PartitionOneNemesis [cluster disrupted]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (locking disrupted
      (case (:f op)
        :start
        (if @disrupted
          (assoc op :type :info :value {:already-partitioned @disrupted})
          (let [candidates (cluster/running-nodes cluster)
                node (when (seq candidates) (rand-nth candidates))]
            (if node
              (do
                (cluster/partition-node! cluster node)
                (reset! disrupted node)
                (assoc op :type :info :value {:partitioned node}))
              (assoc op :type :info :value :no-running-node))))

        :stop
        (if-let [node @disrupted]
          (do
            (cluster/heal-partition! cluster)
            (reset! disrupted nil)
            (assoc op :type :info :value {:healed node}))
          (assoc op :type :info :value :nothing-to-heal)))))

  (teardown! [this _]
    (locking disrupted
      (when @disrupted
        (cluster/heal-partition! cluster)
        (reset! disrupted nil)))
    this)

  nemesis/Reflection
  (fs [_]
    #{:start :stop}))

(defn kill-one
  [cluster]
  (->KillOneNemesis cluster (atom nil)))

(defn pause-one
  [cluster]
  (->PauseOneNemesis cluster (atom nil)))

(defn kill-all
  [cluster]
  (->KillAllNemesis cluster (atom nil)))

(defn pause-all
  [cluster]
  (->PauseAllNemesis cluster (atom nil)))

(defn partition-one
  [cluster]
  (->PartitionOneNemesis cluster (atom nil)))
