(ns register-service.store
  (:require [clojure.core.async :as async :refer [go-loop <! >!! chan]]
            [bookkeeper.client :as bk]
            [zookeeper :as zk]
            [manifold.deferred :as d]
            [cheshire.core :as cheshire])
  (:import [org.apache.zookeeper KeeperException KeeperException$Code]))

(defprotocol Store
  (become-leader! [this])
  (check-and-set! [this expected new])
  (get-value [this])
  (close! [this]))

(defn init-mem-store
  [initial-value]
  (let [register (atom initial-value)]
    (reify Store
      (become-leader! [this]
        (d/success-deferred initial-value))
      (check-and-set! [this expected new]
        (d/success-deferred
         (let [cur @register]
           (if (= cur expected)
             (compare-and-set! register cur new)
             false))))
      (get-value [this]
        (d/success-deferred @register))
      (close! [this]))))

(def register-znode "/registerdata")
(def register-initial-value 0)


(defn write-update
  [ledger value]
  (bk/add-entry ledger
                (.getBytes (cheshire/generate-string {"register-value" value}))))

(defn read-update
  [ledger entry-id]
  (d/chain (bk/read-entries ledger entry-id entry-id)
           (fn [entries]
             (let [data (second (first entries))]
               (cheshire/parse-string (String. data))))
           (fn [map]
             (get map "register-value"))))

(defn read-ledger-list
  [zk]
  (d/chain
   (zk/data zk register-znode :async? true)
   (fn [result]
     (condp = (:return-code result)
       (.intValue KeeperException$Code/NONODE) [nil '()]
       (.intValue KeeperException$Code/OK) [(get-in result [:stat :version])
                                            (cheshire/parse-string
                                             (String. (:data result)))]
       (throw (KeeperException/create
               (KeeperException$Code/get (:return-code result))))))))

(defn write-ledger-list
  [zk ledger-list version]
  (let [bytes (.getBytes
               (cheshire/generate-string ledger-list))]
    (d/chain
     (if (nil? version)
       (do
         (zk/create zk register-znode :persistent? true
                    :async? true :data bytes))
       (zk/set-data zk register-znode bytes version :async? true))
     (fn [result]
       (if (= (:return-code result)
              (.intValue KeeperException$Code/OK))
         true
         (throw (KeeperException/create
                      (KeeperException$Code/get (:return-code result)))))))))

(defn last-register-update
  [bk ledger-list]
  (if (empty? ledger-list)
    (d/success-deferred register-initial-value)
    (d/chain (bk/open-ledger bk (last ledger-list))
             (fn [ledger]
                 (let [lac (bk/last-add-confirmed ledger)]
                   (if (< lac 0)
                     (last-register-update bk (drop-last ledger-list))
                     (read-update ledger lac)))))))

(defn new-ledger
  [zk bk]
  (d/chain
   (d/zip (bk/create-ledger bk) (read-ledger-list zk))
   (fn [[ledger [version, ledger-list]]]
     (d/zip (write-ledger-list zk (concat ledger-list
                                          (list (bk/ledger-id ledger)))
                               version)
            (last-register-update bk ledger-list)
            (d/success-deferred ledger)))
   (fn [[write-list-result last-update ledger]]
     [ledger, last-update])))

(defn- leading-state
  [ledger register-value fatal-error-handler]
  (fn [cmd]
    (case (:action cmd)
      :check-and-set (let [deferred (:deferred cmd)
                           expected (:expected cmd)
                           new (:new cmd)]
                       (if (= register-value expected)
                         (try
                           @(write-update ledger new)
                           (d/success! deferred true)
                           (leading-state ledger new fatal-error-handler)
                           (catch Exception e
                             (fatal-error-handler e)))
                         (do
                           (d/success! deferred false)
                           (leading-state ledger register-value
                                          fatal-error-handler))))
      :get (do
             (d/success! (:deferred cmd) register-value)
             (leading-state ledger register-value
                            fatal-error-handler))
      :shutdown (do
                  ;(close! store)
                  nil))))

(defn- initial-state
  [zk bk fatal-error-handler]
  (fn [cmd]
    (case (:action cmd)
      :become-leader (let [deferred (:deferred cmd)]
                       (try
                         (let [[ledger,last-update] @(new-ledger zk bk)]
                           (d/success! deferred last-update)
                           (leading-state ledger last-update
                                          fatal-error-handler))
                         (catch Exception e
                           (fatal-error-handler e)))))))

(defn init-persistent-store
  "Initialize the store, returning a channel."
  [zk bk fatal-error-handler]
  (let [chan (chan)]
    (go-loop [cmd (<! chan)
              current-state (initial-state zk bk fatal-error-handler)]
      (let [next-state (current-state cmd)]
        (if next-state
          (recur (<! chan) next-state))))

    (reify Store
      (become-leader! [this]
        (let [deferred (d/deferred)]
          (>!! chan {:action :become-leader :deferred deferred})
          deferred))
      (check-and-set! [this expected new]
        (let [deferred (d/deferred)]
          (>!! chan {:action :check-and-set
                     :expected expected
                     :new new
                     :deferred deferred})
          deferred))
      (get-value [this]
        (let [deferred (d/deferred)]
          (>!! chan {:action :get
                     :deferred deferred})
          deferred))
      (close! [this]
        (>!! chan {:action :shutdown})
        (async/close! chan)))))
