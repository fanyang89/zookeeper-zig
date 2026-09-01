(ns zookeeper-zig.docker-cluster-test
  (:require [clojure.test :refer [deftest is]]
            [zookeeper-zig.docker-cluster :as docker-cluster]))

(deftest healing-refuses-stopped-containers
  (let [subject {:nodes [:n1 :n2 :n3]
                 :state (atom {:docker {:initialized? true
                                        :partition :n1}})}]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/ensure-cluster! (fn [_])
       #'zookeeper-zig.docker-cluster/running-nodes (fn [_] [:n1 :n2])}
      #(is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"while Docker nodes are stopped"
            (docker-cluster/heal-partition! subject))))))

(deftest firewall-cleanup-rejects-residual-rules
  (let [reset! (deref
                #'zookeeper-zig.docker-cluster/reset-partition-container!)]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/run-ok? (fn [& _] true)
       #'zookeeper-zig.docker-cluster/run! (fn [& _] "-N ZKJ_PARTITION")}
      #(is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"Failed to remove Docker partition rules"
            (reset! "n1"))))))

(deftest subnet-addresses-are-deterministic
  (is (= "172.20.0.10"
         (docker-cluster/subnet-address "172.20.0.0/16" 10)))
  (is (= "10.42.16.12"
         (docker-cluster/subnet-address "10.42.16.0/20" 12)))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"subnet is too small"
       (docker-cluster/subnet-address "192.0.2.0/30" 3))))
