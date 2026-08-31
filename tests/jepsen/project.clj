(defproject zookeeper-zig-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen linearizability tests for ZooKeeper Zig"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :main zookeeper-zig.core
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [jepsen "0.3.13"]
                 [org.apache.zookeeper/zookeeper "3.9.5"]]
  :jvm-opts ["-Xmx2g"
             "-Djava.awt.headless=true"
             "-Dzookeeper.sasl.client=false"]
  :profiles {:aliyun
             {:mirrors {"central"
                        {:name "aliyun-public"
                         :url "https://maven.aliyun.com/repository/public"}}}})
