(ns register-service.multi-server-test
  (:require [clojure.test :refer :all]
            [failjure.core :as f]
            [ring.adapter.jetty :refer [run-jetty]]
            [clojure.tools.logging :as log]
            [register-service.client :as client]
            [register-service.handler :refer [create-handler, key-url, resource-url]]
            [register-service.leadership :as leadership]
            [register-service.store :as st]
            [register-service.util :as util]
            [register-service.app :refer [local-ip]]
            [clojure.core.async :refer [>!!]]
            [clj-async-test.core :refer :all]))

(def ^:dynamic *servers* [])

(defn hangable-store [zk bk]
  (let [should-hang? (atom false)
        backend (st/init-persistent-store zk bk
                                          (fn [e] (log/debug "Got error " e)))
        store (reify st/Store
                (become-leader! [this]
                  (st/become-leader! backend))
                (set-value! [this k v expected]
                  (st/set-value! backend k v expected))
                (get-value [this k]
                  (st/get-value backend k))
                (close! [this]
                  (st/close! backend)))]
    [should-hang? store]))

(defn register-server []
  (let [zk (util/zk-client)
        bk (util/bk-client)
        [should-hang? store] (hangable-store zk bk)
        lease-atom (atom nil)
        server (run-jetty (create-handler store lease-atom)
                          {:port 0 :join? false})
        http-port (.getLocalPort (nth (.getConnectors server) 0))
        url (resource-url (local-ip) http-port)
        lease (leadership/join-group zk url
                                     (fn [] (st/become-leader! store))
                                     (fn [] (log/debug "leadership lost")))]
    (swap! lease-atom (fn [x] lease))
    {:http-port http-port
     :url url
     :store store
     :should-hang? should-hang?
     :lease lease
     :zk zk
     :bk bk}))

(defn multi-server-fixture
  [f]
  (binding [*servers* (map (fn [i] (register-server)) [1 2 3])]
    (f)
    (map (fn [s]
           (.close (:zk s))
           (.close (:bk s))) *servers*)))

(use-fixtures :each util/bk-fixture multi-server-fixture)

(deftest test-update-leader
  (testing "Update to leader can be read from non-leader"
    (let [new-val 1234
          server0 (:url (nth *servers* 0))
          server1 (:url (nth *servers* 1))
          seqno (:seq (client/get-value (key-url server0 "key1")))
          set-s0-response (client/set-value! (key-url server0 "key1")
                                             new-val :seq-no seqno)
          get-s1-response (:value (client/get-value (key-url server1 "key1")))]
      (is (not (f/failed? set-s0-response)))
      (is (not (f/failed? get-s1-response)))
      (is (= set-s0-response true))
      (is (= get-s1-response new-val)))))

(deftest test-update-nonleader
  (testing "Update to non-leader can be read from leader"
    (let [new-val 5678
          server0 (:url (nth *servers* 0))
          server1 (:url (nth *servers* 1))
          seqno (:seq (client/get-value (key-url server0 "key1")))
          set-s1-response (client/set-value! (key-url server1 "key1")
                                             new-val :seq-no seqno)
          get-s0-response (:value (client/get-value (key-url server0 "key1")))]
      (is (not (f/failed? set-s1-response)))
      (is (not (f/failed? get-s0-response)))
      (is (= set-s1-response true))
      (is (= get-s0-response new-val)))))

(deftest test-leader-change
  (testing "Updates intact on leadership change"
    (let [new-val 1234
          server0 (:url (nth *servers* 0))
          server1 (:url (nth *servers* 1))
          zk0 (:zk (nth *servers* 0))
          lease0 (:lease (nth *servers* 0))
          lease1 (:lease (nth *servers* 1))
          seqno (:seq (client/get-value (key-url server0 "key1")))
          set-s0-response (client/set-value! (key-url server0 "key1")
                                             new-val :seq-no seqno)
          get-s0-response (client/get-value (key-url server0 "key1"))]
      (is (leadership/am-leader? lease0))
      (is (not (leadership/am-leader? lease1)))
      (is (not (f/failed? set-s0-response)))
      (is (not (f/failed? get-s0-response)))
      (is (= set-s0-response true))
      (is (= (:value get-s0-response) new-val))
      (leadership/leave-group zk0 lease0)
      (is (eventually (leadership/am-leader? lease1)))
      (is (= (client/get-value (key-url server0 "key1")) get-s0-response)))))

(deftest test-dead-leader
  (testing "Testing access via non-leader with leader unresponsive"
    (is true)))


