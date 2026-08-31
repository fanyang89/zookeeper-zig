(ns zookeeper-zig.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [zookeeper-zig.core :as core]))

(deftest test-names-identify-configurations
  (testing "the original smoke test keeps its historical name"
    (is (= "zookeeper-zig-register"
           (#'core/test-name "register" "kill-one"))))
  (testing "expanded configurations include workload and nemesis"
    (is (= "zookeeper-zig-presence-pause-one"
           (#'core/test-name "presence" "pause-one")))))

(deftest full-suite-has-bounded-distinct-configurations
  (is (= 5 (count core/full-suite-configurations)))
  (is (= (count core/full-suite-configurations)
         (count (distinct core/full-suite-configurations))))
  (let [configurations (set core/full-suite-configurations)]
    (is (contains? configurations
                   {:workload "independent-register" :nemesis "kill-one"}))
    (is (contains? configurations
                   {:workload "set" :nemesis "kill-one"}))
    (is (contains? configurations
                   {:workload "register" :nemesis "pause-one"}))))

(deftest all-tests-expands-test-count
  (with-redefs [core/zookeeper-test identity]
    (let [tests (doall (core/all-tests {:test-count 2}))]
      (is (= 10 (count tests)))
      (is (every? #(= 1 (:test-count %)) tests)))))
