(ns zookeeper-zig.nemesis-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.nemesis :as nemesis]
            [zookeeper-zig.cluster :as cluster]
            [zookeeper-zig.nemesis :as zk-nemesis]))

(deftest kill-all-disrupts-and-restores-every-running-node
  (let [calls (atom [])
        subject (zk-nemesis/kill-all :cluster)]
    (with-redefs [cluster/running-nodes (fn [_] [:n1 :n2 :n3])
                  cluster/kill-node! (fn [_ node] (swap! calls conj [:kill node]))
                  cluster/start-node! (fn [_ node] (swap! calls conj [:start node]))]
      (is (= {:killed [:n1 :n2 :n3]}
             (:value (nemesis/invoke! subject {} {:type :info :f :start}))))
      (is (= {:restarted [:n1 :n2 :n3]}
             (:value (nemesis/invoke! subject {} {:type :info :f :stop}))))
      (is (= [[:kill :n1] [:kill :n2] [:kill :n3]
              [:start :n1] [:start :n2] [:start :n3]]
             @calls)))))

(deftest partition-one-isolates-and-heals-one-running-node
  (let [calls (atom [])
        subject (zk-nemesis/partition-one :cluster)]
    (with-redefs [cluster/running-nodes (fn [_] [:n1])
                  cluster/partition-node! (fn [_ node]
                                            (swap! calls conj [:partition node]))
                  cluster/heal-partition! (fn [_]
                                            (swap! calls conj [:heal]))]
      (is (= {:partitioned :n1}
             (:value (nemesis/invoke! subject {} {:type :info :f :start}))))
      (is (= {:already-partitioned :n1}
             (:value (nemesis/invoke! subject {} {:type :info :f :start}))))
      (is (= {:healed :n1}
             (:value (nemesis/invoke! subject {} {:type :info :f :stop}))))
      (is (= [[:partition :n1] [:heal]] @calls)))))

(deftest pause-all-disrupts-and-restores-every-running-node
  (let [calls (atom [])
        subject (zk-nemesis/pause-all :cluster)]
    (with-redefs [cluster/running-nodes (fn [_] [:n1 :n2 :n3])
                  cluster/pause-node! (fn [_ node] (swap! calls conj [:pause node]))
                  cluster/resume-node! (fn [_ node] (swap! calls conj [:resume node]))]
      (nemesis/invoke! subject {} {:type :info :f :start})
      (nemesis/invoke! subject {} {:type :info :f :stop})
      (is (= [[:pause :n1] [:pause :n2] [:pause :n3]
              [:resume :n1] [:resume :n2] [:resume :n3]]
             @calls)))))
