(ns register-service.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [register-service.store :as store]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.util.response :refer [response status]]
            [clojure.core.async :refer [>!!]]
            [failjure.core :as f]))

(def resource-path "/register")

(defn resource-url [server port]
  (str "http://" server ":" port resource-path))

(def timeout-ms 5000)

(defn- create-routes
  [chan]
  (routes
   (POST "/register" request
         (let [expected (long (get-in request [:body :expected]))
               new (long (get-in request [:body :new]))
               result (deref (store/check-and-set! chan expected new)
                             timeout-ms (f/fail :timeout))]
           (if (f/failed? result)
             (status (response (str "Failed with: " (f/message result))) 503)
             (response {:updated result}))))
   (GET "/register" []
        (let [result (deref (store/get-value chan)
                            timeout-ms (f/fail :timeout))]
          (if (f/failed? result)
            (status (response (str "Failed with: " (f/message result))) 503)
            (response {:value result}))))
   (route/not-found "Not Found")))

(defn create-handler
  [chan]
  (wrap-json-body
   (wrap-json-response
    (wrap-defaults (create-routes chan) api-defaults))
   {:keywords? true :bigdecimals? true}))
