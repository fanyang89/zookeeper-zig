(ns zookeeper-zig.docker-cluster-test
  (:require [clojure.test :refer [deftest is]]
            [zookeeper-zig.docker-cluster :as docker-cluster])
  (:import (java.io File)))

(deftest docker-command-failures-are-written-to-the-run-log
  (let [log-file (File/createTempFile "zookeeper-zig-docker-" ".log")
        run! (deref #'zookeeper-zig.docker-cluster/run!)]
    (try
      (let [error
            (with-bindings
              {#'zookeeper-zig.docker-cluster/*docker-command-log*
               (.getPath log-file)}
              (with-redefs-fn
                {#'zookeeper-zig.docker-cluster/run-result
                 (fn [_]
                   {:command ["docker" "create" "test-node"]
                    :exit 125
                    :output "container name is already in use"})}
                #(try
                   (run! "create" "test-node")
                   nil
                   (catch clojure.lang.ExceptionInfo caught
                     caught))))]
        (is (= 125 (:exit (ex-data error))))
        (is (not (contains? (ex-data error) :output)))
        (is (re-find #"details logged to" (ex-message error)))
        (let [contents (slurp log-file)]
          (is (re-find #"command: \[\"docker\" \"create\" \"test-node\"\]"
                       contents))
          (is (re-find #"exit: 125" contents))
          (is (re-find #"container name is already in use" contents))))
      (finally
        (.delete log-file)))))

(deftest setup-failure-does-not-clean-uncreated-resources
  (let [subject {:state (atom {})
                 :nodes [:n1]
                 :container-names {:n1 "test-node"}
                 :network-name "test-network"
                 :node-image "missing-image"
                 :run-id "test-run"}
        cleanup-arguments (atom nil)
        ensure-cluster! (deref
                         #'zookeeper-zig.docker-cluster/ensure-cluster!)]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/run!
       (fn [& _]
         (throw (ex-info "missing image" {})))
       #'zookeeper-zig.docker-cluster/cleanup-resources!
       (fn [& arguments]
         (reset! cleanup-arguments arguments))}
      #(is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"missing image"
            (ensure-cluster! subject))))
    (is (= [[] nil nil false] @cleanup-arguments))))

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
