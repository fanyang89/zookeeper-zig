(ns zookeeper-zig.client
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [zookeeper-zig.cluster :as cluster])
  (:import (java.nio.charset StandardCharsets)
           (org.apache.zookeeper CreateMode KeeperException KeeperException$Code
                                 Watcher Watcher$Event$KeeperState ZooDefs$Ids
                                 ZooKeeper)
           (org.apache.zookeeper.data Stat)))

(def register-path "/jepsen-register")
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
  [^ZooKeeper zk]
  (try
    (decode-value (.getData zk register-path false nil))
    (catch org.apache.zookeeper.KeeperException$NoNodeException _
      nil)))

(defn- write-value!
  [^ZooKeeper zk value]
  (let [data (encode-value value)]
    (try
      (.setData zk register-path data -1)
      (catch org.apache.zookeeper.KeeperException$NoNodeException _
        (try
          (.create zk register-path data ZooDefs$Ids/OPEN_ACL_UNSAFE
                   CreateMode/PERSISTENT)
          (catch org.apache.zookeeper.KeeperException$NodeExistsException _
            (.setData zk register-path data -1))))))
  :ok)

(defn- cas-value!
  [^ZooKeeper zk expected new-value]
  (if (nil? expected)
    (try
      (.create zk register-path (encode-value new-value)
               ZooDefs$Ids/OPEN_ACL_UNSAFE CreateMode/PERSISTENT)
      :ok
      (catch org.apache.zookeeper.KeeperException$NodeExistsException _
        :fail))
    (try
      (let [stat (Stat.)
            actual (decode-value (.getData zk register-path false stat))]
        (if (= expected actual)
          (try
            (.setData zk register-path (encode-value new-value) (.getVersion stat))
            :ok
            (catch org.apache.zookeeper.KeeperException$BadVersionException _
              :fail))
          :fail))
      (catch org.apache.zookeeper.KeeperException$NoNodeException _
        :fail))))

(defrecord RegisterClient [cluster zk]
  client/Client
  (open! [this _ _]
    (assoc this :zk (open-zookeeper! (cluster/connect-string cluster))))

  (setup! [this _]
    this)

  (invoke! [_ _ op]
    (try
      (case (:f op)
        :read (assoc op :type :ok :value (read-value zk))
        :write (assoc op :type (write-value! zk (:value op)))
        :cas (let [[expected new-value] (:value op)]
               (assoc op :type (cas-value! zk expected new-value))))
      (catch KeeperException error
        (assoc op
               :type (if (transient-error? error) :info :fail)
               :error (error-keyword error)))
      (catch InterruptedException _
        (.interrupt (Thread/currentThread))
        (assoc op :type :info :error :interrupted))))

  (teardown! [this _]
    this)

  (close! [_ _]
    (when zk
      (.close ^ZooKeeper zk))))

(defn register-client
  [cluster]
  (->RegisterClient cluster nil))
