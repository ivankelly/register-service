(ns register-service.store
  (:require [clojure.core.async :as async :refer [go-loop <! >!! chan]]
            [clojure.tools.logging :as log]
            [bookkeeper.client :as bk]
            [zookeeper :as zk]
            [manifold.deferred :as d]
            [cheshire.core :as cheshire])
  (:import [org.apache.zookeeper KeeperException KeeperException$Code]))

(defprotocol Store
  (become-leader! [this])
  (set-value! [this k v seqno])
  (get-value [this k])
  (close! [this]))

(defn init-mem-store
  [initial-value]
  (let [register (atom initial-value)]
    (reify Store
      (become-leader! [this]
        (d/success-deferred initial-value))
      (set-value! [this k v seqno]
        (d/success-deferred
         (let [cur @register
               cur-seqno (:seq cur)]
           (if (or (nil? seqno) (= cur-seqno seqno))
             (compare-and-set! register cur {:seq (inc cur-seqno)
                                             :value v})
             false))))
      (get-value [this k]
        (d/success-deferred @register))
      (close! [this]))))

(def register-znode "/registerdata")
(def initial-value {})

(defn write-update
  [ledger update]
  (bk/add-entry ledger
                (.getBytes (cheshire/generate-string
                            [(first update)
                             (:seq (second update))
                             (:value (second update))]))))

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

(defn read-full-key-space
  [ledger]
  (let [lac (bk/last-add-confirmed ledger)]
    (if (< lac 0)
      (d/success-deferred {})
      (d/chain (bk/read-entries ledger 0 lac)
               (fn [entries]
                 (map (fn [e]
                        (let [data (second e)
                              values (cheshire/parse-string (String. data))]
                          [(nth values 0)
                           {:seq (nth values 1)
                            :value (nth values 2)}]))
                      entries))
               (fn [entries]
                 (reduce (fn [acc e]
                           (merge acc e)) {} entries))))))

(defn write-full-key-space
  [ledger key-space]
  (->> key-space
       seq
       (map (fn [e]
              (write-update ledger e)))
       last))

(defn read-write-and-return-full-key-space
  [prev-ledger next-ledger]
  (d/chain (read-full-key-space prev-ledger)
           (fn [key-space]
             (d/chain (write-full-key-space next-ledger key-space)
                      (fn [res]
                        key-space)))))

(defn new-ledger
  [zk bk]
  (d/chain
   (d/zip (bk/create-ledger bk) (read-ledger-list zk))
   (fn [[next-ledger [version, ledger-list]]]
     (let [prev-ledger-id (last ledger-list)]
       (d/zip
        (d/success-deferred next-ledger)
        (d/chain (if prev-ledger-id
                   (d/chain (bk/open-ledger bk prev-ledger-id)
                            (fn [prev-ledger]
                              (read-write-and-return-full-key-space
                               prev-ledger next-ledger)))
                   (d/success-deferred {}))
                 (fn [key-space]
                   (d/chain (write-ledger-list zk (concat ledger-list
                                                          (list (bk/ledger-id next-ledger)))
                                               version)
                            (fn [write-result]
                              key-space)))))))))

(defn- leading-state
  [ledger key-space fatal-error-handler]
  (fn [cmd]
    (case (:action cmd)
      :set (let [deferred (:deferred cmd)
                 k (:key cmd)
                 cur-seqno (or (get-in key-space [k :seq]) 0)
                 cmd-seqno (:seq cmd)
                 new-key-value [k {:seq (inc cur-seqno)
                                   :value (:value cmd)}]]
             (if (or (nil? cmd-seqno) (= cur-seqno cmd-seqno))
               (try
                 @(write-update ledger new-key-value)
                 (d/success! deferred true)
                 (leading-state ledger (merge key-space
                                              new-key-value)
                                fatal-error-handler)
                 (catch Exception e
                   (fatal-error-handler e)))
               (do
                 (d/success! deferred false)
                 (leading-state ledger key-space
                                fatal-error-handler))))
      :get (do
             (d/success! (:deferred cmd) (or (get key-space (:key cmd))
                                             {:seq 0 :value 0}))
             (leading-state ledger key-space
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
                         (let [[ledger,key-space] @(new-ledger zk bk)]
                           (d/success! deferred key-space)
                           (leading-state ledger key-space
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
      (set-value! [this k v seqno]
        (let [deferred (d/deferred)]
          (>!! chan {:action :set
                     :seq seqno
                     :key k
                     :value v
                     :deferred deferred})
          deferred))
      (get-value [this k]
        (let [deferred (d/deferred)]
          (>!! chan {:action :get
                     :key k
                     :deferred deferred})
          deferred))
      (close! [this]
        (>!! chan {:action :shutdown})
        (async/close! chan)))))
