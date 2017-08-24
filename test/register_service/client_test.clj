(ns register-service.client-test
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [clojure.test :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.util.response :refer [response status]]
            [register-service.app :refer [local-ip]]
            [register-service.client :as client]
            [register-service.handler :refer [resource-url]]
            [failjure.core :as f]))

(def ^:dynamic *jetty-port* nil)

(def trigger-timeout-seq 1234)

(defn- timeout-or-error [should-timeout]
  (if (not should-timeout)
    (status (response "Some random error") 503)
    (Thread/sleep 1000000)))

(defn- create-error-routes
  "Errors by default.
  Once trigger timeout value received hangs on all requests"
  []
  (let [should-timeout (atom false)]
    (routes
     (POST "/register" request
           (let [seqno (long (get-in request [:body :seq]))]
             (if (= seqno trigger-timeout-seq)
               (swap! should-timeout (fn [x] true))))
           (timeout-or-error @should-timeout))
     (GET "/register" []
          (timeout-or-error @should-timeout))
     (route/not-found "Not Found"))))

(defn create-error-handler []
  (wrap-json-body
   (wrap-json-response
    (wrap-defaults (create-error-routes) api-defaults))
   {:keywords? true :bigdecimals? true}))

(defn error-fixture
  [f]
  (let [server (run-jetty (create-error-handler)
                          {:port 0 :join? false})]
    (binding [*jetty-port* (.getLocalPort (nth (.getConnectors server) 0))]
      (f))))

(use-fixtures :each error-fixture)

(deftest test-get-and-set-errors
  (testing "Get and set operation errors"
    (let [url (resource-url (local-ip) *jetty-port*)]
      (let [response (client/get-value url)]
        (is (f/failed? response)))
      (let [response (client/set-value! url 100 :seq-no 0)]
        (is (f/failed? response)))
      (let [response (client/set-value! url 100 :seq-no trigger-timeout-seq)]
        (is (f/failed? response)))
      (let [response (client/get-value url)]
        (is (f/failed? response))))))

