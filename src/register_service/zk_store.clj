(ns register-service.zk-store
  (:require [zookeeper :as zk]
            [zookeeper.util :as util]
            [register-service.store :as store])
  (:import [java.net DatagramSocket InetAddress]))

(def root-znode "/register/leadership")

(defn node-from-path [path]
  (.substring path (inc (count root-znode))))

(declare elect-leader)

(defn watch-predecessor [client me pred leader current-leader {:keys [event-type path]}]
  (println me ": watcher triggering " event-type " " path " " leader)
  (if (and (= event-type :NodeDeleted) (= (node-from-path path) leader))
    (do
      (println "I am the leader!")
      (swap! current-leader (fn [x] me)))
    (if-not (zk/exists client (str root-znode "/" pred)
                       :watcher (partial watch-predecessor client me pred leader current-leader))
      (elect-leader client me current-leader))))

(defn predecessor [me coll]
  (ffirst (filter #(= (second %) me) (partition 2 1 coll))))

(defn elect-leader [client me current-leader]
  (let [members (util/sort-sequential-nodes (zk/children client root-znode))
        leader (first members)]
    (swap! current-leader (fn [x] leader))
    (print "I am" me ", current leader " leader)
    (if (= me leader)
      (println " and I am the leader!")
      (let [pred (predecessor me members)]
        (println " and my predecessor is:" pred)
        (if-not (zk/exists client (str root-znode "/" pred)
                           :watcher (partial watch-predecessor client me pred leader current-leader))
          (elect-leader client me current-leader))))))

(defn join-group [client]
  (let [current-leader (atom nil)
        me (node-from-path (zk/create-all client (str root-znode "/n-")
                                          :sequential? true))]
    (elect-leader client me current-leader)
    {:node me
     :current-leader current-leader}))

(defn leave-group [client lease]
  (zk/delete client (str root-znode "/" (:node lease))))

(defn- local-ip []
  (let [socket (DatagramSocket.)]
    (.connect socket (InetAddress/getByName "8.8.8.8") 0)
    (.getHostAddress (.getLocalAddress socket))))

(defn zk-store
  [connect-str]
  (let [client (zk/connect connect-str)]
    (join-group client)
    (reify
      store/Store
      (check-and-set! [this expected new promise])
      (get-value [this promise])
      (close! [this]))))

