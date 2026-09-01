(ns zookeeper-zig.report
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io PushbackReader]
           [java.nio.file Files StandardCopyOption]
           [java.time Instant]))

(defn- html-escape
  [value]
  (str/escape (str value)
              {\& "&amp;"
               \< "&lt;"
               \> "&gt;"
               \" "&quot;"
               \' "&#39;"}))

(defn- read-results
  [file]
  (with-open [reader (PushbackReader. (io/reader file))]
    (edn/read {:default (fn [_ value] value)} reader)))

(defn- result-status
  [directory]
  (let [results-file (io/file directory "results.edn")]
    (if (.isFile results-file)
      (try
        (case (:valid? (read-results results-file))
          true {:label "Valid" :class "valid"}
          false {:label "Invalid" :class "invalid"}
          {:label "Unknown" :class "unknown"})
        (catch Exception _
          {:label "Unreadable" :class "invalid"}))
      {:label "Incomplete" :class "incomplete"})))

(defn- result-directories
  [store]
  (->> (file-seq store)
       (filter #(.isFile %))
       (filter #(contains? #{"results.edn" "jepsen.log"} (.getName %)))
       (map #(.getParentFile %))
       (map #(.getCanonicalFile %))
       distinct
       (sort-by #(.getPath %))))

(defn- copy-result!
  [store output directory]
  (let [relative (.relativize (.toPath store) (.toPath directory))
        destination (.resolve (.toPath output) relative)]
    (Files/createDirectories destination (make-array java.nio.file.attribute.FileAttribute 0))
    (doseq [file (.listFiles directory)
            :when (.isFile file)]
      (Files/copy (.toPath file)
                  (.resolve destination (.getName file))
                  (into-array StandardCopyOption
                              [StandardCopyOption/REPLACE_EXISTING])))
    (.toString relative)))

(defn- report-link
  [relative directory file label]
  (when (.isFile (io/file directory file))
    (format "<a href=\"%s/%s\">%s</a>"
            (html-escape relative)
            (html-escape file)
            (html-escape label))))

(defn- chart
  [relative directory file alt]
  (when (.isFile (io/file directory file))
    (format (str "<a href=\"%s/%s\"><img loading=\"lazy\" "
                 "src=\"%s/%s\" alt=\"%s\"></a>")
            (html-escape relative)
            (html-escape file)
            (html-escape relative)
            (html-escape file)
            (html-escape alt))))

(defn- report-card
  [store output directory]
  (let [relative (copy-result! store output directory)
        parts (str/split relative #"[/\\]")
        test-name (or (first parts) relative)
        timestamp (or (second parts) "unknown")
        status (result-status directory)
        links (keep identity
                    [(report-link relative directory "timeline.html" "Timeline")
                     (report-link relative directory "results.edn" "Results")
                     (report-link relative directory "history.txt" "History")
                     (report-link relative directory "jepsen.log" "Log")])
        charts (keep identity
                     [(chart relative directory "rate.png" "Operation rate")
                      (chart relative directory "latency-quantiles.png" "Latency quantiles")
                      (chart relative directory "latency-raw.png" "Raw latency")])]
    (format (str "<article class=\"card\">"
                 "<div class=\"card-header\"><div><h2>%s</h2><p>%s</p></div>"
                 "<span class=\"status %s\">%s</span></div>"
                 "<nav>%s</nav><div class=\"charts\">%s</div></article>")
            (html-escape test-name)
            (html-escape timestamp)
            (html-escape (:class status))
            (html-escape (:label status))
            (str/join "" links)
            (str/join "" charts))))

(defn- actions-url
  []
  (let [server (System/getenv "GITHUB_SERVER_URL")
        repository (System/getenv "GITHUB_REPOSITORY")
        run-id (System/getenv "GITHUB_RUN_ID")]
    (when (and server repository run-id)
      (str server "/" repository "/actions/runs/" run-id))))

(defn- document
  [cards]
  (let [run-url (actions-url)
        run-id (System/getenv "GITHUB_RUN_ID")
        run-attempt (System/getenv "GITHUB_RUN_ATTEMPT")
        run-link (when run-url
                   (format "<a class=\"run-link\" href=\"%s\">Actions run</a>"
                           (html-escape run-url)))
        run-label (when run-id
                    (str "Run " (html-escape run-id)
                         (when run-attempt
                           (str " · Attempt " (html-escape run-attempt)))))]
    (str "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
         "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
         "<title>Jepsen Report</title><style>"
         ":root{color-scheme:light dark;font-family:ui-sans-serif,system-ui,sans-serif}"
         "body{max-width:1200px;margin:0 auto;padding:32px 20px;background:#f6f8fa;color:#1f2328}"
         "header{display:flex;align-items:end;justify-content:space-between;gap:16px;margin-bottom:24px}"
         "h1,h2,p{margin:0}h1{font-size:32px}header p,.card p{color:#59636e;margin-top:6px}"
         ".run-link,nav a{display:inline-block;text-decoration:none;font-weight:600;color:#0969da}"
         ".grid{display:grid;gap:18px}.card{background:#fff;border:1px solid #d0d7de;border-radius:12px;padding:20px;box-shadow:0 1px 2px #1f23280a}"
         ".card-header{display:flex;align-items:start;justify-content:space-between;gap:16px}.card h2{font-size:20px}"
         ".status{border-radius:999px;padding:5px 10px;font-size:13px;font-weight:700}.valid{background:#dafbe1;color:#116329}.invalid{background:#ffebe9;color:#cf222e}.unknown,.incomplete{background:#fff8c5;color:#7d4e00}"
         "nav{display:flex;flex-wrap:wrap;gap:16px;margin:18px 0}.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.charts img{display:block;width:100%;border:1px solid #d8dee4;border-radius:8px}"
         ".empty{background:#fff;border:1px solid #d0d7de;border-radius:12px;padding:28px}"
         "footer{color:#59636e;font-size:13px;margin-top:24px}"
         "@media(prefers-color-scheme:dark){body{background:#0d1117;color:#e6edf3}.card,.empty{background:#161b22;border-color:#30363d}header p,.card p,footer{color:#8d96a0}.run-link,nav a{color:#58a6ff}.charts img{border-color:#30363d}}"
         "</style></head><body><header><div><h1>Jepsen Report</h1>"
         (when run-label (str "<p>" run-label "</p>"))
         "</div>" run-link "</header><main class=\"grid\">"
         (if (seq cards)
           (str/join "" cards)
           "<p class=\"empty\">No Jepsen results were produced.</p>")
         "</main><footer>Generated " (html-escape (Instant/now)) "</footer>"
         "</body></html>")))

(defn generate!
  [store-path output-path]
  (let [store (.getCanonicalFile (io/file store-path))
        output (.getCanonicalFile (io/file output-path))]
    (when (.exists output)
      (doseq [file (reverse (file-seq output))]
        (io/delete-file file true)))
    (.mkdirs output)
    (let [cards (mapv #(report-card store output %)
                      (if (.isDirectory store)
                        (result-directories store)
                        []))]
      (spit (io/file output "index.html") (document cards))
      (spit (io/file output ".nojekyll") "")
      {:runs (count cards)
       :output (.getPath output)})))

(defn -main
  [& [store output]]
  (let [result (generate! (or store "store") (or output "report"))]
    (println (str "Generated " (:runs result) " Jepsen report(s) in " (:output result)))))
