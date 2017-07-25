(ns register-service.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [register-service.store :as store]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.util.response :refer [response]]
            [clojure.core.async :refer [>!!]]))

(defn- create-routes
  [chan]
  (routes
   (POST "/register" request
         (let [expected (long (get-in request [:body :expected]))
               new (long (get-in request [:body :new]))]
           (response (deref (store/check-and-set! chan expected new)
                            5000 {:result :timeout}))))
   (GET "/register" []
        (response (deref (store/get-value chan) 5000 {:result :timeout})))
   (route/not-found "Not Found")))

(defn create-handler
  [chan]
  (wrap-json-body
   (wrap-json-response
    (wrap-defaults (create-routes chan) api-defaults))
   {:keywords? true :bigdecimals? true}))
