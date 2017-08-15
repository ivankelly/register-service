(ns register-service.store-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [close!]]
            [register-service.store :as store]
            [register-service.util :as util]
            [bookkeeper.client :as bk]
            [bookkeeper.mini-cluster :as mc]
            [failjure.core :as f]
            [zookeeper :as zk])
  (:import [org.apache.zookeeper KeeperException]))

(use-fixtures :each util/bk-fixture)

(deftest test-store
  (testing "check-and-set and get"
    (let [zk (zk/connect util/*zkconnect*)
          bk (bk/bookkeeper {:zookeeper/connect util/*zkconnect*})
          store (store/init-persistent-store zk bk
                                             (fn [e] (println "Got error " e)))]
      @(store/become-leader! store)
      (is (= @(store/get-value store) 0))
      (is (= @(store/check-and-set! store 0 100) true))
      (is (= @(store/check-and-set! store 0 200) false))
      (is (= @(store/check-and-set! store 100 200) true))
      (store/close! store))))

(deftest test-store-read-write
  (testing "Reading and writing entries to a ledger"
    (let [bk (bk/bookkeeper {:zookeeper/connect util/*zkconnect*})
          ledger @(bk/create-ledger bk)
          last-value 0xdeadbeef]
      (store/write-update ledger 123)
      (store/write-update ledger 234)
      (let [last-entry-id @(store/write-update ledger last-value)]
        (is (= @(store/read-update ledger last-entry-id) last-value))))))

(deftest test-read-write-list
  (testing "Reading and writing a list of entries to zookeeper"
    (let [zk (zk/connect util/*zkconnect*)
          to-write '(1 2 3)]
      (let [[version,ledgers] @(store/read-ledger-list zk)]
        (println version)
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
    (let [zk (zk/connect util/*zkconnect*)
          bk (bk/bookkeeper {:zookeeper/connect util/*zkconnect*})
          test-value 0xcafebeef]
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value 0)))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value 0))
        @(store/write-update ledger test-value))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value test-value)))
      (let [[ledger,value] @(store/new-ledger zk bk)]
        (is (= value test-value))))))
