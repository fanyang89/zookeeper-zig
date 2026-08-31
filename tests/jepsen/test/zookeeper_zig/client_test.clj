(ns zookeeper-zig.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [zookeeper-zig.client :as client]))

(deftest independent-register-paths-are-stable
  (testing "numeric keys map to distinct root znodes"
    (is (= "/jepsen-register-0" (client/register-path-for-key 0)))
    (is (= "/jepsen-register-42" (client/register-path-for-key 42)))
    (is (not= (client/register-path-for-key 1)
              (client/register-path-for-key 2)))))
