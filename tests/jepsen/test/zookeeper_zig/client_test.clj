(ns zookeeper-zig.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [zookeeper-zig.client :as client])
  (:import (org.apache.zookeeper KeeperException KeeperException$Code)))

(deftest independent-register-paths-are-stable
  (testing "numeric keys map to distinct root znodes"
    (is (= "/jepsen-register-0" (client/register-path-for-key 0)))
    (is (= "/jepsen-register-42" (client/register-path-for-key 42)))
    (is (not= (client/register-path-for-key 1)
              (client/register-path-for-key 2)))))

(deftest sequential-ids-use-zookeeper-suffix
  (is (= 0 (client/sequential-id "/jepsen-sequence-0000000000")))
  (is (= 42 (client/sequential-id "/jepsen-sequence-0000000042"))))

(deftest queue-values-map-to-child-paths
  (is (= "/jepsen-queue/0000000042"
         (client/queue-child-path "0000000042"))))

(deftest queue-checkers-use-safe-uncertain-dequeue-types
  (is (= :ok (:uncertain-dequeue-type (client/total-queue-client nil))))
  (is (= :info (:uncertain-dequeue-type (client/linear-queue-client nil))))
  (let [uncertain-dequeue (deref #'zookeeper-zig.client/uncertain-dequeue)
        op {:type :invoke :f :dequeue :value nil}]
    (is (= {:type :ok :f :dequeue :value "0000000042" :error :interrupted}
           (uncertain-dequeue op :ok "0000000042" :interrupted)))
    (is (= :info
           (:type (uncertain-dequeue op :info "0000000042"
                                     :connectionloss))))))

(deftest final-queue-drain-retries-transient-errors
  (let [attempts (atom 0)
        connection-loss (KeeperException/create
                         KeeperException$Code/CONNECTIONLOSS)
        drain! (deref #'zookeeper-zig.client/drain-queue!)
        result (with-redefs-fn
                 {#'zookeeper-zig.client/drain-queue-once!
                  (fn [_]
                    (if (< (swap! attempts inc) 3)
                      (throw connection-loss)
                      #{"0000000042"}))
                  #'zookeeper-zig.client/queue-drain-retry-ms 0}
                 #(drain! nil {:type :invoke :f :drain :value nil}))]
    (is (= 3 @attempts))
    (is (= {:type :ok :f :drain :value #{"0000000042"}} result))))

(deftest final-queue-drain-never-returns-info
  (let [connection-loss (KeeperException/create
                         KeeperException$Code/CONNECTIONLOSS)
        drain! (deref #'zookeeper-zig.client/drain-queue!)
        result (with-redefs-fn
                 {#'zookeeper-zig.client/drain-queue-once!
                  (fn [_] (throw connection-loss))
                  #'zookeeper-zig.client/queue-drain-attempts 2
                  #'zookeeper-zig.client/queue-drain-retry-ms 0}
                 #(drain! nil {:type :invoke :f :drain :value nil}))]
    (is (= :fail (:type result)))
    (is (= :connectionloss (:error result)))))
