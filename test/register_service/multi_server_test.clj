(ns register-service.multi-server-test
  (:require [clojure.test :refer :all]
            [failjure.core :as f]
            [ring.adapter.jetty :refer [run-jetty]]
            [register-service.client :as client]
            [register-service.handler :refer [create-handler, resource-url]]
            [register-service.leadership :as leadership]
            [register-service.store :as st]
            [register-service.app :refer [local-ip]]
            [clojure.core.async :refer [>!!]]
            [bookkeeper.mini-cluster :as mc]
            [zookeeper :as zk]))

(def ^:dynamic *zkconnect* nil)
(def ^:dynamic *servers* [])

(defn bk-fixture
  [f]
  (let [cluster (mc/create 0)] ; 0 bookies, we only want zk
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)]
      (f))
    (mc/kill cluster)))

(defn register-server [zk-connect]
  (let [store-chan (st/init-store)
        zk (zk/connect *zkconnect*)
        lease-atom (atom nil)
        server (run-jetty (create-handler store-chan lease-atom)
                          {:port 0 :join? false})
        http-port (.getLocalPort (nth (.getConnectors server) 0))
        url (resource-url (local-ip) http-port)
        lease (leadership/join-group zk url
                                     (fn [] (st/become-leader store-chan)))]
    (swap! lease-atom (fn [x] lease))
    {:http-port http-port
     :url url
     :store-chan store-chan}))

(defn multi-server-fixture
  [f]
  (binding [*servers* (map (fn [i] (register-server *zkconnect*)) [1 2 3])]
    (f)))

(use-fixtures :each bk-fixture multi-server-fixture)

(deftest test-multi-server
  (testing "Update to leader can be read from non-leader"
    (let [new-val 1234
          server0 (:url (nth *servers* 0))
          server1 (:url (nth *servers* 1))
          current (client/get-value server0)
          set-s0-response (client/check-and-set! server0 current new-val)
          get-s1-response (client/get-value server1)]
      (is (not (f/failed? set-s0-response)))
      (is (not (f/failed? get-s1-response)))
      (is (= set-s0-response true))
      (is (= get-s1-response new-val))))
  (testing "Update to non-leader can be read from leader"
    (let [new-val 5678
          server0 (:url (nth *servers* 0))
          server1 (:url (nth *servers* 1))
          current (client/get-value server0)
          set-s1-response (client/check-and-set! server1 current new-val)
          get-s0-response (client/get-value server0)]
      (is (not (f/failed? set-s1-response)))
      (is (not (f/failed? get-s0-response)))
      (is (= set-s1-response true))
      (is (= get-s0-response new-val))))
  (testing "Testing access via non-leader with leader unresponsive"
    (is true))
  (testing "Updates intact on leadership change"))

