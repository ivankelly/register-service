(ns register-service.leadership-test
  (:require [clojure.test :refer :all]
            [register-service.leadership :as leadership]
            [register-service.util :as util]
            [clj-async-test.core :refer :all]))

(use-fixtures :each util/bk-fixture)

(deftest test-leadership
  (testing "Leadership failover works"
    (let [client1 (util/zk-client)
          client2 (util/zk-client)
          client3 (util/zk-client)
          data1 "data1"
          data2 "data2"
          data3 "data3"
          lease1 (leadership/join-group client1 data1)
          lease2 (leadership/join-group client2 data2)
          lease3 (leadership/join-group client3 data3)]
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease1))))
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease2))))
      (is (eventually (= {:node (:node lease1) :data data1}
                         @(:current-leader lease3))))

      (leadership/leave-group client1 lease1)
      (is (eventually (= {:node (:node lease2) :data data2}
                         @(:current-leader lease2))))
      (is (eventually (= {:node (:node lease2) :data data2}
                         @(:current-leader lease3))))

      (leadership/leave-group client2 lease2)
      (is (eventually (= {:node (:node lease3) :data data3}
                         @(:current-leader lease3)))))))

(deftest test-leader-callback
  (testing "Callback only called once"
    (let [data (map #(str "data" %) (range 0 10))
          atoms (map (fn [x] (atom 0)) data)
          cbs (map (fn [a] (fn [] (swap! a inc))) atoms)
          clients (map (fn [x] (util/zk-client)) atoms)
          leases (map (fn [c d cb]
                        (leadership/join-group c d cb (fn [])))
                      clients data cbs)]
      (is (eventually (leadership/am-leader? (first leases))))
      (is (= @(first atoms) 1))
      (is (= (reduce + (map deref atoms)) 1))
      (leadership/leave-group (second clients) (second leases))
      (is (= @(first atoms) 1))
      (is (= (reduce + (map deref atoms)) 1))
      (leadership/leave-group (first clients) (first leases))
      (is (eventually (leadership/am-leader? (nth leases 2))))
      (is (= @(first atoms) 1))
      (is (= @(nth atoms 2) 1))
      (is (= (reduce + (map deref atoms)) 2))
      (leadership/leave-group (last clients) (last leases))
      (is (= @(first atoms) 1))
      (is (= @(nth atoms 2) 1))
      (is (= (reduce + (map deref atoms)) 2)))))


(deftest test-leadership-loss-callback
  (testing "Leadership failover works"
    (let [client1 (util/zk-client)
          client2 (util/zk-client)
          atom1 (atom 0)
          atom2 (atom 0)
          cb1 (fn [] (swap! atom1 inc))
          cb2 (fn [] (swap! atom2 inc))
          lease1 (leadership/join-group client1 ""
                                        (fn []) cb1)
          lease2 (leadership/join-group client2 ""
                                        (fn []) cb2)]
      (is (eventually (leadership/am-leader? lease1)))
      (is (and (= @atom1 0) (= @atom2 0)))
      (leadership/leave-group client1 lease1)
      (is (eventually (leadership/am-leader? lease2)))
      (is (eventually (= @atom1 1)))
      (is (= @atom2 0)))))
