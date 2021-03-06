(ns register-service.leadership
  (:require [zookeeper :as zk]
            [zookeeper.util :as util])
  (:import [org.apache.zookeeper KeeperException$NoNodeException]))

(def election-znode "/register/leadership")

(defn node-from-path [path]
  (.substring path (inc (count election-znode))))

(defn elect-leader
  [client me current-leader
   on-leadership on-leadership-loss]
  (let [watcher (fn [{:keys [event-type path]}]
                  (elect-leader client me current-leader
                                on-leadership on-leadership-loss))
        members (util/sort-sequential-nodes
                 (zk/children client election-znode :watcher watcher))
        leader (first members)
        data (try
               (String. (:data (zk/data client
                                        (str election-znode "/" leader))))
               (catch KeeperException$NoNodeException e :data-error))]
    (if (= data :data-error)
      (recur client me current-leader on-leadership on-leadership-loss)
      (do
        (if (and (not (= (:node @current-leader) leader)) (= leader me))
          (on-leadership))
        (if (and (= (:node @current-leader) me) (not (= leader me)))
          (on-leadership-loss))
        (swap! current-leader (fn [x] {:node leader :data data}))))))

(defn join-group
  ([client data]
   (join-group client data (fn []) (fn [])))
  ([client data on-leadership on-leadership-loss]
   (let [current-leader (atom nil)
         me (node-from-path (zk/create-all client (str election-znode "/n-")
                                           :sequential? true
                                           :data (.getBytes data)))]
     (elect-leader client me current-leader on-leadership on-leadership-loss)
     {:node me
      :current-leader current-leader})))

(defn leave-group [client lease]
  (zk/delete client (str election-znode "/" (:node lease))))

(defn am-leader? [lease]
  (and lease
       (= (:node @(:current-leader lease)) (:node lease))))

(defn leader-data [lease]
  (if lease
    (:data @(:current-leader lease))))

(def always-leader {:node "foobar" :current-leader (atom {:node "foobar"})})
