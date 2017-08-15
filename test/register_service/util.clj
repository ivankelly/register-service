(ns register-service.util
  (:require [bookkeeper.mini-cluster :as mc]))

(def ^:dynamic *zkconnect* nil)

(defn bk-fixture
  [f]
  (let [cluster (mc/create 3)]
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)]
      (f))
    (mc/kill cluster)))
