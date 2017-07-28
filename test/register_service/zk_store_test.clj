(ns register-service.zk-store-test
  (:require [clojure.test :refer :all]
            [register-service.zk-store :as store]
            [zookeeper :as zk]
            [bookkeeper.mini-cluster :as mc]
            [clj-async-test.core :refer :all]))

(def ^:dynamic *zkconnect* nil)

(defn bk-fixture
  [f]
  (let [cluster (mc/create 0)] ; 0 bookies, we only want zk
    (mc/start cluster)
    (binding [*zkconnect* (mc/zookeeper-connect-string cluster)]
      (f))
    (mc/kill cluster)))

(use-fixtures :each bk-fixture)

(deftest test-leadership
  (testing "Leadership failover works"
    (let [client1 (zk/connect *zkconnect*)
          client2 (zk/connect *zkconnect*)
          client3 (zk/connect *zkconnect*)
          lease1 (store/join-group client1)
          lease2 (store/join-group client2)
          lease3 (store/join-group client3)]
      (is (eventually (= (:node lease1) @(:current-leader lease1))))
      (is (eventually (= (:node lease1) @(:current-leader lease2))))
      (is (eventually (= (:node lease1) @(:current-leader lease3))))

      (println "killing lease 1")
      (store/leave-group client1 lease1)
      (is (eventually (= (:node lease2) @(:current-leader lease2))))
      (is (eventually (= (:node lease2) @(:current-leader lease3))))

      (store/leave-group client2 lease2)
      (is (eventually (= (:node lease3) @(:current-leader lease3))))

      (println lease3)
      )))
