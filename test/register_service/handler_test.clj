(ns register-service.handler-test
  (:require [clojure.test :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.mock.request :as mock]
            [register-service.app :refer [local-ip]]
            [register-service.client :as client]
            [register-service.handler :refer :all]
            [register-service.leadership :as lead]
            [register-service.store :as st]))

(def ^:dynamic *jetty-port* nil)

(defn jetty-fixture
  [f]
  (let [store-chan (st/init-store)
        leader lead/always-leader
        server (run-jetty (create-handler store-chan (atom leader))
                          {:port 0 :join? false})]
    (binding [*jetty-port* (.getLocalPort (nth (.getConnectors server) 0))]
      (f))))

(use-fixtures :each jetty-fixture)

(deftest test-get-and-set
  (testing "Get and set operations"
    (let [url (resource-url (local-ip) *jetty-port*)]
      (let [response (client/get-value url)]
        (is (= response 0)))
      (let [response (client/check-and-set! url 0 100)]
        (is response))
      (let [response (client/check-and-set! url 0 100)]
        (is (not response))))))
