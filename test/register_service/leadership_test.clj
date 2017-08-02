(ns register-service.leadership
  (:require [clojure.test :refer :all]
            [register-service.leadership :as store]
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
          data1 "data1"
          data2 "data2"
          data3 "data3"
          lease1 (store/join-group client1 data1)
          lease2 (store/join-group client2 data2)
          lease3 (store/join-group client3 data3)]
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease1))))
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease2))))
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease3))))

      (store/leave-group client1 lease1)
      (is (eventually (= {:node (:node lease2) :data data2}
                         @(:current-leader lease2))))
      (is (eventually (= {:node (:node lease2) :data data2}
                         @(:current-leader lease3))))

      (store/leave-group client2 lease2)
      (is (eventually (= {:node (:node lease3) :data data3}
                         @(:current-leader lease3)))))))
