(ns zookeeper-zig.core
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.tests :as tests]
            [knossos.model :as model]
            [zookeeper-zig.client :as zk-client]
            [zookeeper-zig.cluster :as cluster]
            [zookeeper-zig.db :as zk-db]
            [zookeeper-zig.nemesis :as zk-nemesis])
  (:import (java.io File)))

(def workload-names ["register" "independent-register" "set" "presence"])
(def nemesis-names ["kill-one" "pause-one"])
(def full-suite-configurations
  [{:workload "register" :nemesis "kill-one"}
   {:workload "presence" :nemesis "kill-one"}
   {:workload "independent-register" :nemesis "kill-one"}
   {:workload "set" :nemesis "kill-one"}
   {:workload "register" :nemesis "pause-one"}])
(def independent-threads-per-key 3)

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

(defn lifecycle-write-op
  [_ _]
  {:type :invoke :f :write :value (rand-nth [nil true])})

(defn lifecycle-cas-op
  [_ _]
  {:type :invoke
   :f :cas
   :value (rand-nth [[nil true] [true nil]])})

(defn keyed-read-op
  [_ _]
  {:type :invoke :f :read :value (independent/tuple 0 nil)})

(defn- nemesis-cycle
  []
  (cycle [(gen/sleep 5)
          {:type :info :f :start}
          (gen/sleep 5)
          {:type :info :f :stop}]))

(defn- fault-workload
  [client-generator final-generator time-limit]
  (gen/phases
   (->> client-generator
        (gen/stagger 0.02)
        (gen/nemesis (nemesis-cycle))
        (gen/time-limit time-limit))
   (gen/nemesis (gen/once {:type :info :f :stop}))
   (gen/log "Waiting for the cluster to stabilize")
   (gen/sleep 3)
   (gen/clients final-generator)))

(defn- register-workload
  [time-limit]
  {:client-generator (gen/mix [read-op write-op cas-op cas-op])
   :final-generator (gen/once read-op)
   :checker (checker/linearizable {:model (model/cas-register)})
   :client-fn zk-client/register-client})

(defn- independent-register-workload
  [time-limit]
  {:client-generator (independent/concurrent-generator
                      independent-threads-per-key
                      (range)
                      (fn [_]
                        (gen/mix [read-op write-op cas-op cas-op])))
   :final-generator (gen/once keyed-read-op)
   :checker (independent/checker
             (checker/linearizable {:model (model/cas-register)}))
   :client-fn zk-client/independent-register-client})

(defn- set-workload
  [time-limit]
  (let [next-element (atom -1)
        add-op (fn [_ _]
                 {:type :invoke
                  :f :add
                  :value (swap! next-element inc)})]
    {:client-generator (gen/mix [add-op add-op add-op read-op])
     :final-generator (gen/once read-op)
     :checker (checker/set-full {:linearizable? true})
     :client-fn zk-client/set-client}))

(defn- presence-workload
  [time-limit]
  {:client-generator (gen/mix [read-op lifecycle-write-op
                               lifecycle-cas-op lifecycle-cas-op])
   :final-generator (gen/once read-op)
   :checker (checker/linearizable {:model (model/cas-register)})
   :client-fn zk-client/presence-client})

(defn- workload
  [name time-limit]
  (let [spec (case name
               "register" (register-workload time-limit)
               "independent-register" (independent-register-workload time-limit)
               "set" (set-workload time-limit)
               "presence" (presence-workload time-limit))]
    (assoc spec
           :generator (fault-workload (:client-generator spec)
                                      (:final-generator spec)
                                      time-limit))))

(defn- nemesis
  [name cluster]
  (case name
    "kill-one" (zk-nemesis/kill-one cluster)
    "pause-one" (zk-nemesis/pause-one cluster)))

(defn- test-name
  [workload-name nemesis-name]
  (if (and (= "register" workload-name) (= "kill-one" nemesis-name))
    "zookeeper-zig-register"
    (str "zookeeper-zig-" workload-name "-" nemesis-name)))

(defn zookeeper-test
  [opts]
  (let [binary (System/getenv "ZOOKEEPER_ZIG_SERVER")
        run-root (or (System/getenv "ZOOKEEPER_ZIG_RUN_DIR") "target")
        nodes (:nodes opts)
        workload-name (:workload opts)
        nemesis-name (:nemesis opts)
        binary-file (when binary (File. binary))]
    (when-not (and binary-file (.isFile binary-file) (.canExecute binary-file))
      (throw (ex-info "ZOOKEEPER_ZIG_SERVER must name an executable server binary"
                      {:value binary})))
    (when-not (and (<= 3 (count nodes)) (odd? (count nodes)))
      (throw (ex-info "Jepsen requires an odd cluster of at least three nodes"
                      {:nodes nodes})))
    (when (and (= "independent-register" workload-name)
               (not (zero? (mod (:concurrency opts)
                                independent-threads-per-key))))
      (throw (ex-info "Independent register concurrency must be divisible by three"
                      {:concurrency (:concurrency opts)})))
    (let [cluster (cluster/cluster binary nodes run-root)
          workload-spec (workload workload-name (:time-limit opts))]
      (info "ZooKeeper Zig Jepsen data directory:" (str (:root cluster)))
      (merge tests/noop-test
             opts
             {:name (test-name workload-name nemesis-name)
              :pure-generators true
              :db (zk-db/->LocalDB cluster)
              :client ((:client-fn workload-spec) cluster)
              :nemesis (nemesis nemesis-name cluster)
              :checker (checker/compose
                        {:perf (checker/perf)
                         :workload (:checker workload-spec)
                         :timeline (timeline/html)})
              :generator (:generator workload-spec)}))))

(def jepsen-opt-spec
  [[nil "--workload NAME"
    (str "Workload: " (str/join ", " workload-names))
    :default "register"
    :validate [#(some #{%} workload-names) "Unknown workload"]]
   [nil "--nemesis NAME"
    (str "Nemesis: " (str/join ", " nemesis-names))
    :default "kill-one"
    :validate [#(some #{%} nemesis-names) "Unknown nemesis"]]])

(defn all-tests
  [opts]
  (for [_ (range (:test-count opts))
        configuration full-suite-configurations]
    (zookeeper-test (merge opts configuration {:test-count 1}))))

(defn -main
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn zookeeper-test
                                :opt-spec jepsen-opt-spec})
          (cli/test-all-cmd {:tests-fn all-tests
                             :opt-spec jepsen-opt-spec}))
   args))
