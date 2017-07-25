(ns register-service.store-test
  (:require [clojure.test :refer :all]
            [register-service.store :as store]
            [clojure.core.async :as async :refer [close!]]))

(deftest test-store
  (testing "check-and-set and get"
    (let [store (store/init-store)]
      (is (= (:value (deref (store/get-value store) 1000 -1)) 0))
      (is (= (:value (deref (store/check-and-set! store 0 100) 1000 -1)) 100))
      (is (= (:result (deref (store/check-and-set! store 0 100) 1000 :timeout))
             :error))
      (is (= (:value (deref (store/check-and-set! store 100 200) 1000 -1)) 200))
      (store/shutdown! store))))
