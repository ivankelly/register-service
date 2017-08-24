(ns register-service.handler-test
  (:require [clojure.test :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.mock.request :as mock]
            [register-service.app :refer [local-ip]]
            [register-service.client :as client]
            [register-service.handler :refer :all]
            [register-service.leadership :as lead]
            [register-service.store :as st]
            [failjure.core :as f]))

(def ^:dynamic *jetty-port* nil)

(defn jetty-fixture
  [f]
  (let [store (st/init-mem-store {:seq 0 :value 0})
        leader lead/always-leader
        server (run-jetty (create-handler store (atom leader))
                          {:port 0 :join? false})]
    @(st/become-leader! store)
    (binding [*jetty-port* (.getLocalPort (nth (.getConnectors server) 0))]
      (f))))

(use-fixtures :each jetty-fixture)

(deftest test-get-and-set
  (testing "Get and set operations"
    (let [url (resource-url (local-ip) *jetty-port*)]
      (let [response (client/get-value url 0)]
        (is (= response {:seq 0 :value 0})))
      (let [response (client/set-value! url 100 0)]
        (is response))
      (let [response (client/set-value! url 100 0)]
        (is (not response))))))

(deftest test-set-without-seq
  (testing "Set without sequence numbers"
    (let [url (resource-url (local-ip) *jetty-port*)
          set-response-1 (client/set-value! url 1234)
          get-response-1 (client/get-value url 0)
          set-response-2 (client/set-value! url 4321)
          get-response-2 (client/get-value url 0)]
      (is (= set-response-1 true))
      (is (= (:value get-response-1) 1234))
      (is (= set-response-2 true))
      (is (= (:value get-response-2) 4321)))))

(deftest test-get-newer-seq
  (testing "Get value with a sequence higher than exists on the server"
    (let [url (resource-url (local-ip) *jetty-port*)]
      (let [response (client/get-value url 0)]
        (is (= response {:seq 0 :value 0})))
      (let [response (client/get-value url 10)]
        (is (f/failed? response)))
      (let [response (client/get-value url 0)]
        (is (= response {:seq 0 :value 0}))))))
