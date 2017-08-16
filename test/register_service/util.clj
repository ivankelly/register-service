(ns register-service.util
  (:require [bookkeeper.client :as bk]
            [bookkeeper.mini-cluster :as mc]
            [zookeeper :as zk]))

(def ^:dynamic *zkconnect* nil)
(def ^:dynamic *cluster* nil)
(def ^:dynamic *cleanups* nil)

(defn bk-fixture
  [f]
  (let [cluster (mc/create 3)]
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)
              *cluster* cluster
              *cleanups* (atom [])]
      (f)
      (map (fn [f] (apply f [])) *cleanups*))
    (mc/kill cluster)))

(defn kill-bookies
  []
  (.killBookie *cluster* 0)
  (.killBookie *cluster* 0)
  (.killBookie *cluster* 0))

(defn zk-client []
  (let [zk (zk/connect *zkconnect*)]
    (swap! *cleanups* #(cons (fn [] (.close zk)) %))
    zk))

(defn bk-client []
  (let [bk (bk/bookkeeper {:zookeeper/connect *zkconnect*})]
    (swap! *cleanups* #(cons (fn [] (bk/close bk)) %))
    bk))
