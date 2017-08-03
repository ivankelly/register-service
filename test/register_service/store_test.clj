(ns register-service.store-test
  (:require [clojure.test :refer :all]
            [register-service.store :as store]
            [clojure.core.async :as async :refer [close!]]))

(deftest test-store
  (testing "check-and-set and get"
    (let [store (store/init-store)]
      (store/become-leader store)
      (is (= @(store/get-value store) 0))
      (is (= @(store/check-and-set! store 0 100) true))
      (is (= @(store/check-and-set! store 0 200) false))
      (is (= @(store/check-and-set! store 100 200) true))
      (store/shutdown! store))))
