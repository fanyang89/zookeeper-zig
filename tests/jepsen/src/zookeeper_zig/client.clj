(ns zookeeper-zig.client
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [zookeeper-zig.cluster :as cluster])
  (:import (java.nio.charset StandardCharsets)
           (org.apache.zookeeper CreateMode KeeperException KeeperException$Code
                                 Watcher Watcher$Event$KeeperState ZooDefs$Ids
                                 ZooKeeper)
           (org.apache.zookeeper.data Stat)))

(def register-path "/jepsen-register")
(def independent-register-prefix "/jepsen-register-")
(def set-path "/jepsen-set")
(def presence-path "/jepsen-presence")
(def unique-id-prefix "/jepsen-sequence-")
(def counter-path "/jepsen-counter")
(def counter-entry-prefix "entry-")
(def session-timeout-ms 10000)
(def connect-timeout-ms 30000)

(defn- encode-value
  [value]
  (.getBytes (str value) StandardCharsets/UTF_8))

(defn- decode-value
  [data]
  (Long/parseLong (String. ^bytes data StandardCharsets/UTF_8)))

(defn- error-keyword
  [^KeeperException error]
  (-> error .code .name str/lower-case keyword))

(defn- transient-error?
  [^KeeperException error]
  (contains? #{KeeperException$Code/CONNECTIONLOSS
               KeeperException$Code/OPERATIONTIMEOUT
               KeeperException$Code/REQUESTTIMEOUT
               KeeperException$Code/SESSIONEXPIRED
               KeeperException$Code/SESSIONMOVED}
             (.code error)))

(defn- open-zookeeper!
  [connect-string]
  (let [connected (promise)
        watcher (reify Watcher
                  (process [_ event]
                    (when (= Watcher$Event$KeeperState/SyncConnected
                             (.getState event))
                      (deliver connected true))))
        zk (ZooKeeper. connect-string session-timeout-ms watcher)]
    (if (deref connected connect-timeout-ms false)
      zk
      (do
        (.close zk)
        (throw (ex-info "ZooKeeper client did not connect"
                        {:connect-string connect-string}))))))

(defn- read-value
  [^ZooKeeper zk path]
  (try
    (decode-value (.getData zk path false nil))
    (catch org.apache.zookeeper.KeeperException$NoNodeException _
      nil)))

(defn- write-value!
  [^ZooKeeper zk path value]
  (let [data (encode-value value)]
    (try
      (.setData zk path data -1)
      (catch org.apache.zookeeper.KeeperException$NoNodeException _
        (try
          (.create zk path data ZooDefs$Ids/OPEN_ACL_UNSAFE
                   CreateMode/PERSISTENT)
          (catch org.apache.zookeeper.KeeperException$NodeExistsException _
            (.setData zk path data -1))))))
  :ok)

(defn- cas-value!
  [^ZooKeeper zk path expected new-value]
  (if (nil? expected)
    (try
      (.create zk path (encode-value new-value)
               ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/PERSISTENT)
      :ok
      (catch org.apache.zookeeper.KeeperException$NodeExistsException _
        :fail))
    (try
      (let [stat (Stat.)
            actual (decode-value (.getData zk path false stat))]
        (if (= expected actual)
          (try
            (.setData zk path (encode-value new-value) (.getVersion stat))
            :ok
            (catch org.apache.zookeeper.KeeperException$BadVersionException _
              :fail))
          :fail))
      (catch org.apache.zookeeper.KeeperException$NoNodeException _
        :fail))))

(defn register-path-for-key
  [key]
  (str independent-register-prefix key))

(defn sequential-id
  [path]
  (Long/parseLong (subs path (- (count path) 10))))

(defn- invoke-register!
  [^ZooKeeper zk path op]
  (case (:f op)
    :read (assoc op :type :ok :value (read-value zk path))
    :write (assoc op :type (write-value! zk path (:value op)))
    :cas (let [[expected new-value] (:value op)]
           (assoc op :type (cas-value! zk path expected new-value)))))

(defn- invoke-client!
  [op f]
  (try
    (f)
    (catch KeeperException error
      (assoc op
             :type (if (transient-error? error) :info :fail)
             :error (error-keyword error)))
    (catch InterruptedException _
      (.interrupt (Thread/currentThread))
      (assoc op :type :info :error :interrupted))))

(defrecord RegisterClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (invoke-client! op #(invoke-register! zk register-path op)))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defrecord IndependentRegisterClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (invoke-client!
     op
     #(let [[k value] (:value op)
            path (register-path-for-key k)
            logical-op (assoc op :value value)
            result (invoke-register! zk path logical-op)]
        (assoc result :value (independent/tuple k (:value result))))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defn- ensure-set-root!
  [^ZooKeeper zk]
  (try
    (.create zk set-path (byte-array 0) ZooDefs$Ids/OPEN_ACL_UNSAFE
             CreateMode/PERSISTENT)
    (catch org.apache.zookeeper.KeeperException$NodeExistsException _
      set-path)))

(defn- add-element!
  [^ZooKeeper zk element]
  (ensure-set-root! zk)
  (try
    (.create zk (str set-path "/" element) (byte-array 0)
             ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/PERSISTENT)
    (catch org.apache.zookeeper.KeeperException$NodeExistsException _
      nil))
  :ok)

(defn- read-set
  [^ZooKeeper zk]
  (try
    (->> (.getChildren zk set-path false)
         (map #(Long/parseLong %))
         set)
    (catch org.apache.zookeeper.KeeperException$NoNodeException _
      #{})))

(defrecord SetClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    (ensure-set-root! zk)
    this)

  (invoke! [_ _ op]
    (invoke-client!
     op
     #(case (:f op)
        :add (assoc op :type (add-element! zk (:value op)))
        :read (assoc op :type :ok :value (read-set zk)))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defn- presence-value
  [^ZooKeeper zk]
  (when (.exists zk presence-path false)
    true))

(defn- create-presence!
  [^ZooKeeper zk]
  (try
    (.create zk presence-path (byte-array 0) ZooDefs$Ids/OPEN_ACL_UNSAFE
             CreateMode/PERSISTENT)
    (catch org.apache.zookeeper.KeeperException$NodeExistsException _
      nil))
  :ok)

(defn- delete-presence!
  [^ZooKeeper zk]
  (try
    (.delete zk presence-path -1)
    (catch org.apache.zookeeper.KeeperException$NoNodeException _
      nil))
  :ok)

(defn- cas-presence!
  [^ZooKeeper zk expected new-value]
  (cond
    (= expected new-value)
    (if (= expected (presence-value zk)) :ok :fail)

    (and (nil? expected) (= true new-value))
    (try
      (.create zk presence-path (byte-array 0) ZooDefs$Ids/OPEN_ACL_UNSAFE
               CreateMode/PERSISTENT)
      :ok
      (catch org.apache.zookeeper.KeeperException$NodeExistsException _
        :fail))

    (and (= true expected) (nil? new-value))
    (try
      (let [stat (.exists zk presence-path false)]
        (if stat
          (try
            (.delete zk presence-path (.getVersion ^Stat stat))
            :ok
            (catch org.apache.zookeeper.KeeperException$BadVersionException _
              :fail)
            (catch org.apache.zookeeper.KeeperException$NoNodeException _
              :fail))
          :fail))
      (catch org.apache.zookeeper.KeeperException$NoNodeException _
        :fail))

    :else
    :fail))

(defrecord UniqueIdsClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (invoke-client!
     op
     #(let [path (.create zk unique-id-prefix (byte-array 0)
                          ZooDefs$Ids/OPEN_ACL_UNSAFE
                          CreateMode/PERSISTENT_SEQUENTIAL)]
        (assoc op :type :ok :value (sequential-id path)))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defn- ensure-counter-root!
  [^ZooKeeper zk]
  (try
    (.create zk counter-path (byte-array 0) ZooDefs$Ids/OPEN_ACL_UNSAFE
             CreateMode/PERSISTENT)
    (catch org.apache.zookeeper.KeeperException$NodeExistsException _
      counter-path)))

(defn- add-to-counter!
  [^ZooKeeper zk value]
  (ensure-counter-root! zk)
  (.create zk (str counter-path "/" counter-entry-prefix) (encode-value value)
           ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/PERSISTENT_SEQUENTIAL)
  :ok)

(defn- read-counter
  [^ZooKeeper zk]
  (try
    (reduce + 0
            (map #(read-value zk (str counter-path "/" %))
                 (.getChildren zk counter-path false)))
    (catch org.apache.zookeeper.KeeperException$NoNodeException _
      0)))

(defrecord CounterClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    (ensure-counter-root! zk)
    this)

  (invoke! [_ _ op]
    (invoke-client!
     op
     #(case (:f op)
        :read (assoc op :type :ok :value (read-counter zk))
        :final-read (assoc op :type :ok :value (read-counter zk))
        :add (assoc op :type (add-to-counter! zk (:value op))))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defrecord PresenceClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (invoke-client!
     op
     #(case (:f op)
        :read (assoc op :type :ok :value (presence-value zk))
        :write (assoc op :type (if (:value op)
                                 (create-presence! zk)
                                 (delete-presence! zk)))
        :cas (let [[expected new-value] (:value op)]
               (assoc op :type (cas-presence! zk expected new-value))))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defn register-client
  [cluster]
  (->RegisterClient cluster nil))

(defn independent-register-client
  [cluster]
  (->IndependentRegisterClient cluster nil))

(defn set-client
  [cluster]
  (->SetClient cluster nil))

(defn presence-client
  [cluster]
  (->PresenceClient cluster nil))

(defn unique-ids-client
  [cluster]
  (->UniqueIdsClient cluster nil))

(defn counter-client
  [cluster]
  (->CounterClient cluster nil))
