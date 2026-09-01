(ns zookeeper-zig.report-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [zookeeper-zig.report :as report])
  (:import [java.nio.file Files]))

(defn- delete-tree!
  [directory]
  (doseq [file (reverse (file-seq directory))]
    (io/delete-file file true)))

(deftest generate-report-index
  (let [root (.toFile (Files/createTempDirectory
                       "zookeeper-zig-report-test"
                       (make-array java.nio.file.attribute.FileAttribute 0)))
        store (io/file root "store")
        output (io/file root "report")
        run (io/file store "zookeeper-zig-register" "20260101T000000.000Z")]
    (try
      (.mkdirs run)
      (spit (io/file run "results.edn")
            "{:model #knossos.model.CASRegister{:value 0}, :valid? true}")
      (spit (io/file run "timeline.html") "<html>timeline</html>")
      (spit (io/file run "jepsen.log") "complete")
      (is (= 1 (:runs (report/generate! store output))))
      (let [index (slurp (io/file output "index.html"))]
        (is (str/includes? index "zookeeper-zig-register"))
        (is (str/includes? index "class=\"status valid\">Valid"))
        (is (str/includes? index
                           "zookeeper-zig-register/20260101T000000.000Z/timeline.html")))
      (is (.isFile (io/file output
                            "zookeeper-zig-register"
                            "20260101T000000.000Z"
                            "timeline.html")))
      (finally
        (delete-tree! root)))))

(deftest generate-empty-report
  (let [root (.toFile (Files/createTempDirectory
                       "zookeeper-zig-empty-report-test"
                       (make-array java.nio.file.attribute.FileAttribute 0)))
        output (io/file root "report")]
    (try
      (is (= 0 (:runs (report/generate! (io/file root "missing") output))))
      (is (str/includes? (slurp (io/file output "index.html"))
                         "No Jepsen results were produced."))
      (finally
        (delete-tree! root)))))
