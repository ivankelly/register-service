(ns register-service.store
  (:require [clojure.core.async :as async :refer [go-loop <! >!! chan]]
            [failjure.core :as f]))

(defprotocol Store
  (check-and-set! [this expected new promise])
  (get-value [this promise])
  (close! [this]))

(defn- in-mem-store
  []
  (let [register (atom 0)]
    (reify Store
      (check-and-set! [this expected new promise]
        (deliver promise (compare-and-set! register expected new)))
      (get-value [this promise]
        (deliver promise @register))
      (close! [this]))))

(defn- handle-cmd
  [store cmd]
  (case (:action cmd)
    :check-and-set (check-and-set! store
                                   (:expected cmd)
                                   (:new cmd)
                                   (:promise cmd))
    :get (get-value store (:promise cmd))
    :shutdown (close! store)
    (let [promise (:promise promise)]
      (if promise
        (deliver promise (f/fail :unknown-cmd)))
      (println "Unknown cmd" cmd)))
  (:action cmd))

(defn init-store
  "Initialize the store, returning a channel."
  []
  (let [c (chan)
        store (in-mem-store)]
    (go-loop [cmd (<! c)]
      (if-not (or (nil? cmd) (= (handle-cmd store cmd) :shutdown))
        (recur (<! c))))
    c))

(defn check-and-set!
  [chan expected new]
  (let [promise (promise)]
    (>!! chan {:action :check-and-set
               :expected expected
               :new new
               :promise promise})
    promise))

(defn get-value
  [chan]
  (let [promise (promise)]
    (>!! chan {:action :get
               :promise promise})
    promise))

(defn shutdown!
  [chan]
  (>!! chan {:action :shutdown})
  (async/close! chan))
