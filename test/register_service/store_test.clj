(ns register-service.store-test
  (:require [clojure.test :refer :all]
            [clj-async-test.core :refer :all]
            [clojure.tools.logging :as log]
            [register-service.store :as store]
            [register-service.util :as util]
            [bookkeeper.client :as bk])
  (:import [org.apache.zookeeper KeeperException]))

(use-fixtures :each util/bk-fixture)

(deftest test-store
  (testing "check-and-set and get"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          k "key1"
          store (store/init-persistent-store zk bk
                                             (fn [e] (log/debug "Got error " e)))]
      @(store/become-leader! store)
      (is (= @(store/get-value store k) {:seq 0 :value 0}))
      (is (= @(store/set-value! store k 100 0) true))
      (is (= @(store/set-value! store k 200 0) false))
      (is (= @(store/set-value! store k 200 1) true))
      (store/close! store))))

(deftest test-multi-key
  (testing "check-and-set with multiple keys"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          k1 "key1"
          k2 "key2"
          store (store/init-persistent-store zk bk
                                             (fn [e] (log/debug "Got error " e)))]
      @(store/become-leader! store)
      (is (= @(store/get-value store k1) {:seq 0 :value 0}))
      (is (= @(store/get-value store k2) {:seq 0 :value 0}))
      (is (= @(store/set-value! store k1 100 0) true))
      (is (= @(store/set-value! store k2 200 0) true))
      (is (= @(store/set-value! store k1 300 0) false))
      (is (= @(store/set-value! store k2 400 0) false))
      (is (= @(store/get-value store k1) {:seq 1 :value 100}))
      (is (= @(store/get-value store k2) {:seq 1 :value 200}))
      (store/close! store))))

(deftest test-store-read-keyspace
  (testing "Reading from a ledger list gives the full keyspace"
    (let [bk (util/bk-client)
          ledger @(bk/create-ledger bk)]
      (store/write-update ledger ["key1" {:seq 120 :value 123}])
      (store/write-update ledger ["key2" {:seq 121 :value 234}])
      (store/write-update ledger ["key1" {:seq 120 :value 321}])
      (is (= @(store/read-full-key-space
               @(bk/open-ledger bk (bk/ledger-id ledger)))
             {"key1" {:seq 120 :value 321}
              "key2" {:seq 121 :value 234}})))))

(deftest test-store-read-write-read-keyspace
  (testing "Reading from a ledger list gives the full keyspace"
    (let [bk (util/bk-client)
          ledger @(bk/create-ledger bk)]
      (store/write-update ledger ["key1" {:seq 120 :value 123}])
      (store/write-update ledger ["key2" {:seq 121 :value 234}])
      @(store/write-update ledger ["key1" {:seq 120 :value 321}])
      (let [prev-ledger @(bk/open-ledger bk (bk/ledger-id ledger))
            next-ledger @(bk/create-ledger bk)
            key-space @(store/read-write-and-return-full-key-space
                        prev-ledger next-ledger)
            read-back @(store/read-full-key-space
                        @(bk/open-ledger bk (bk/ledger-id next-ledger)))
            should-match {"key1" {:seq 120 :value 321}
                          "key2" {:seq 121 :value 234}}]
        (is (= should-match key-space))
        (is (= should-match read-back))))))

(deftest test-store-write-change-leader-read
  (testing "Keys are available after leader changes"
    (let [zk (util/zk-client)
          bk (util/bk-client)
          store1 (store/init-persistent-store
                  zk bk (fn [e] (log/debug "Got error " e)))
          store2 (store/init-persistent-store
                  zk bk (fn [e] (log/debug "Got error " e)))
          store3 (store/init-persistent-store
                  zk bk (fn [e] (log/debug "Got error " e)))]
      @(store/become-leader! store1)
      (is (= @(store/set-value! store1 "key1" 100 0) true))
      (is (= @(store/set-value! store1 "key2" 200 0) true))
      @(store/become-leader! store2)
      (is (= @(store/get-value store2 "key1") {:seq 1 :value 100}))
      (is (= @(store/get-value store2 "key2") {:seq 1 :value 200}))
      @(store/become-leader! store3)
      (is (= @(store/get-value store3 "key1") {:seq 1 :value 100}))
      (is (= @(store/get-value store3 "key2") {:seq 1 :value 200}))
      (store/close! store1)
      (store/close! store2)
      (store/close! store3))))

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
          test-value ["keyX" {:seq 1 :value 0xcafebeef}]]
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value {})))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value {}))
        @(store/write-update ledger test-value))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= (first (seq value)) test-value)))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= (first (seq value)) test-value))))))

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
          k "key1"
          triggered (atom false)
          handler (fn [e] (swap! triggered (fn [x] true)))
          store (store/init-persistent-store zk bk handler)]
      @(store/become-leader! store)
      (is (= @(store/set-value! store k 1 0) true))
      (util/kill-bookies)
      (is (not @triggered))
      (store/set-value! store k 2 1)
      (is (eventually @triggered)))))
