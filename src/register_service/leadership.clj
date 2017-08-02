(ns register-service.leadership
  (:require [zookeeper :as zk]
            [zookeeper.util :as util])
  (:import [org.apache.zookeeper KeeperException$NoNodeException]))

(def election-znode "/register/leadership")

(defn node-from-path [path]
  (.substring path (inc (count election-znode))))

(defn elect-leader [client me current-leader]
  (let [watcher (fn [{:keys [event-type path]}]
                  (elect-leader client me current-leader))
        members (util/sort-sequential-nodes
                 (zk/children client election-znode :watcher watcher))
        leader (first members)
        data (try
               (String. (:data (zk/data client
                                        (str election-znode "/" leader))))
               (catch KeeperException$NoNodeException e :data-error))]
    (if (= data :data-error)
      (recur client me current-leader)
      (swap! current-leader (fn [x] {:node leader :data data})))))

(defn join-group [client data]
  (let [current-leader (atom nil)
        me (node-from-path (zk/create-all client (str election-znode "/n-")
                                          :sequential? true
                                          :data (.getBytes data)))]
    (elect-leader client me current-leader)
    {:node me
     :current-leader current-leader}))

(defn leave-group [client lease]
  (zk/delete client (str election-znode "/" (:node lease))))

(defn am-leader? [lease]
  nil)

(defn leader-data [lease]
  (:data @(:current-leader lease)))

