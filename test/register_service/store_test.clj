(ns register-service.store-test
  (:require [clojure.test :refer :all]
            [clj-async-test.core :refer :all]
            [register-service.store :as store]
            [register-service.util :as util]
            [bookkeeper.client :as bk])
  (:import [org.apache.zookeeper KeeperException]))

(use-fixtures :each util/bk-fixture)

(deftest test-store
  (testing "check-and-set and get"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          store (store/init-persistent-store zk bk
                                             (fn [e] (println "Got error " e)))]
      @(store/become-leader! store)
      (is (= @(store/get-value store) {:seq 0 :value 0}))
      (is (= @(store/set-value! store 100 0) true))
      (is (= @(store/set-value! store 200 0) false))
      (is (= @(store/set-value! store 200 1) true))
      (store/close! store))))

(deftest test-store-read-write
  (testing "Reading and writing entries to a ledger"
    (let [bk (util/bk-client)
          ledger @(bk/create-ledger bk)
          last-value {:seq 123 :value 0xdeadbeef}]
      (store/write-update ledger {:seq 120 :value 123})
      (store/write-update ledger {:seq 121 :value 234})
      (let [last-entry-id @(store/write-update ledger last-value)]
        (is (= @(store/read-update ledger last-entry-id) last-value))))))

(deftest test-read-write-list
  (testing "Reading and writing a list of entries to zookeeper"
    (let [zk (util/zk-client)
          to-write '(1 2 3)]
      (let [[version,ledgers] @(store/read-ledger-list zk)]
        (is (= ledgers '())))
      (is (thrown? KeeperException @(store/write-ledger-list zk '(1 2 3) 3)))
      (is @(store/write-ledger-list zk to-write nil))
      (let [[version,ledgers] @(store/read-ledger-list zk)]
        (is version)
        (is @(store/write-ledger-list zk (concat ledgers '(4)) version)))
      (let [[version,ledgers] @(store/read-ledger-list zk)]
        (is (= ledgers '(1 2 3 4)))))))

(deftest test-acquire-log
  (testing "Acquiring a new ledger in the log"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          test-value {:seq 1 :value 0xcafebeef}]
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value store/register-initial-value)))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value store/register-initial-value))
        @(store/write-update ledger test-value))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value test-value)))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value test-value))))))

(deftest test-fatal-error-before-leadership
  (testing "Fatal error occurs before the store can become leader"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          triggered (atom false)
          handler (fn [e] (swap! triggered (fn [x] true)))
          store (store/init-persistent-store zk bk handler)]
      (util/kill-bookies)
      (store/become-leader! store)
      (is (eventually @triggered)))))

(deftest test-fatal-error-as-leader
  (testing "Fatal error occurs when store is leader"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          triggered (atom false)
          handler (fn [e] (swap! triggered (fn [x] true)))
          store (store/init-persistent-store zk bk handler)]
      @(store/become-leader! store)
      (is (= @(store/set-value! store 1 0) true))
      (util/kill-bookies)
      (is (not @triggered))
      (store/set-value! store 2 1)
      (is (eventually @triggered)))))
