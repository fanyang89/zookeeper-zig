(ns zookeeper-zig.db
  (:require [jepsen.db :as db]
            [zookeeper-zig.cluster :as cluster]))

(defrecord LocalDB [cluster]
  db/DB
  (setup! [_ _ node]
    (cluster/start-node! cluster node))
  (teardown! [_ _ node]
    (cluster/teardown-node! cluster node))

  db/Kill
  (kill! [_ _ node]
    (cluster/kill-node! cluster node))
  (start! [_ _ node]
    (cluster/start-node! cluster node)))
