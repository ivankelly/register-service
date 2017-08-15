(ns register-service.util
  (:require [bookkeeper.mini-cluster :as mc]))

(def ^:dynamic *zkconnect* nil)
(def ^:dynamic *cluster* nil)

(defn bk-fixture
  [f]
  (let [cluster (mc/create 3)]
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)
              *cluster* cluster]
      (f))
    (mc/kill cluster)))

(defn kill-bookies
  []
  (.killBookie *cluster* 0)
  (.killBookie *cluster* 0)
  (.killBookie *cluster* 0))
