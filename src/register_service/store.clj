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
        (deliver promise (let [cur @register]
                           (if (= cur expected)
                             (compare-and-set! register cur new)
                             false))))
      (get-value [this promise]
        (deliver promise @register))
      (close! [this]))))

(defn- leading-state
  [store]
  (fn [cmd]
    (case (:action cmd)
      :check-and-set (do
                       (check-and-set! store
                                       (:expected cmd)
                                       (:new cmd)
                                       (:promise cmd))
                       (leading-state store))
      :get (do
             (get-value store (:promise cmd))
             (leading-state store))
      :shutdown (do
                  (close! store)
                  nil))))

(defn- initial-state
  [cmd]
  (case (:action cmd)
    :become-leader (let [store (in-mem-store)]
                     (leading-state store))))

(defn init-store
  "Initialize the store, returning a channel."
  []
  (let [c (chan)]
    (go-loop [cmd (<! c)
              current-state initial-state]
      (let [next-state (current-state cmd)]
        (if next-state
          (recur (<! c) next-state))))
    c))

(defn become-leader
  "Your the leader now, dog"
  [chan]
  (>!! chan {:action :become-leader}))

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
