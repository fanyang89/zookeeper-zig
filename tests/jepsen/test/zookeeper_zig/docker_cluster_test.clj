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

(deftest docker-network-is-recreated-with-an-explicit-subnet
  (let [calls (atom [])
        create-network! (deref
                         #'zookeeper-zig.docker-cluster/create-static-ip-network!)]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/run!
       (fn [& arguments]
         (swap! calls conj (vec arguments))
         (when (= ["network" "inspect"] (take 2 arguments))
           "172.19.0.0/16"))}
      #(is (= "172.19.0.0/16"
              (create-network! "test-network" "test-label"))))
    (is (= [["network" "create" "--label" "test-label" "test-network"]
            ["network" "inspect" "--format"
             "{{(index .IPAM.Config 0).Subnet}}" "test-network"]
            ["network" "rm" "test-network"]
            ["network" "create" "--subnet" "172.19.0.0/16"
             "--label" "test-label" "test-network"]]
           @calls))))

(deftest partition-probe-checks-both-sides-of-the-cut
  (let [subject {:nodes [:n1 :n2 :n3]
                 :container-names {:n1 "c1" :n2 "c2" :n3 "c3"}
                 :state (atom {:docker {:addresses {:n1 "172.20.0.10"
                                                    :n2 "172.20.0.11"
                                                    :n3 "172.20.0.12"}}})}
        calls (atom [])
        verify! (deref #'zookeeper-zig.docker-cluster/verify-partition!)]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/run-result
       (fn [arguments]
         (swap! calls conj (vec arguments))
         {:exit 1 :output "Operation not permitted"})
       #'zookeeper-zig.docker-cluster/dropped-packets
       (fn [container]
         ({"c1" 2 "c2" 1 "c3" 1} container))}
      #(is (= {:n1 2 :n2 1 :n3 1}
              (verify! subject :n1 [:n2 :n3]))))
    (is (= #{["exec" "c1" "bash" "-c"
              "printf x > /dev/udp/172.20.0.11/7000"]
             ["exec" "c2" "bash" "-c"
              "printf x > /dev/udp/172.20.0.10/7000"]
             ["exec" "c1" "bash" "-c"
              "printf x > /dev/udp/172.20.0.12/7000"]
             ["exec" "c3" "bash" "-c"
              "printf x > /dev/udp/172.20.0.10/7000"]}
           (set @calls)))))

(deftest partition-probe-rejects-an-unverified-node
  (let [subject {:nodes [:n1 :n2 :n3]
                 :container-names {:n1 "c1" :n2 "c2" :n3 "c3"}
                 :state (atom {:docker {:addresses {:n1 "172.20.0.10"
                                                    :n2 "172.20.0.11"
                                                    :n3 "172.20.0.12"}}})}
        verify! (deref #'zookeeper-zig.docker-cluster/verify-partition!)]
    (with-redefs-fn
      {#'zookeeper-zig.docker-cluster/run-result
       (fn [_]
         {:exit 1 :output "Operation not permitted"})
       #'zookeeper-zig.docker-cluster/dropped-packets
       (fn [container]
         ({"c1" 2 "c2" 0 "c3" 1} container))}
      #(is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"did not block probe traffic"
            (verify! subject :n1 [:n2 :n3]))))))

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
