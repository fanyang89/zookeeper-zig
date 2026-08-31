(ns zookeeper-zig.core
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [zookeeper-zig.client :as zk-client]
            [zookeeper-zig.cluster :as cluster]
            [zookeeper-zig.db :as zk-db]
            [zookeeper-zig.nemesis :as zk-nemesis])
  (:import (java.io File)))

(defn read-op
  [_ _]
  {:type :invoke :f :read :value nil})

(defn write-op
  [_ _]
  {:type :invoke :f :write :value (rand-int 5)})

(defn cas-op
  [_ _]
  {:type :invoke
   :f :cas
   :value [(rand-nth [nil 0 1 2 3 4]) (rand-int 5)]})

(defn- workload
  [time-limit]
  (gen/phases
   (->> (gen/mix [read-op write-op cas-op cas-op])
        (gen/stagger 0.02)
        (gen/nemesis
         (cycle [(gen/sleep 5)
                 {:type :info :f :start}
                 (gen/sleep 5)
                 {:type :info :f :stop}]))
        (gen/time-limit time-limit))
   (gen/nemesis (gen/once {:type :info :f :stop}))
   (gen/log "Waiting for the cluster to stabilize")
   (gen/sleep 3)
   (gen/clients (gen/once read-op))))

(defn zookeeper-test
  [opts]
  (let [binary (System/getenv "ZOOKEEPER_ZIG_SERVER")
        run-root (or (System/getenv "ZOOKEEPER_ZIG_RUN_DIR") "target")
        nodes (:nodes opts)
        binary-file (when binary (File. binary))]
    (when-not (and binary-file (.isFile binary-file) (.canExecute binary-file))
      (throw (ex-info "ZOOKEEPER_ZIG_SERVER must name an executable server binary"
                      {:value binary})))
    (when-not (and (<= 3 (count nodes)) (odd? (count nodes)))
      (throw (ex-info "Jepsen requires an odd cluster of at least three nodes"
                      {:nodes nodes})))
    (let [cluster (cluster/cluster binary nodes run-root)]
      (info "ZooKeeper Zig Jepsen data directory:" (str (:root cluster)))
      (merge tests/noop-test
             opts
             {:name "zookeeper-zig-register"
              :pure-generators true
              :db (zk-db/->LocalDB cluster)
              :client (zk-client/register-client cluster)
              :nemesis (zk-nemesis/kill-one cluster)
              :checker (checker/compose
                        {:perf (checker/perf)
                         :linearizable (checker/linearizable
                                        {:model (model/cas-register)})
                         :timeline (timeline/html)})
              :generator (workload (:time-limit opts))}))))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn zookeeper-test}) args))
