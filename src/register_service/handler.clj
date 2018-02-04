(ns register-service.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [register-service.client :as client]
            [register-service.leadership :as lead]
            [register-service.store :as store]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.util.response :refer [response status]]
            [clojure.core.async :refer [>!!]]
            [failjure.core :as f]))

(def resource-path "/registers")

(defn key-url
  [server k]
  (str server "/" k))

(defn resource-url
  ([server port]
   (str "http://" server ":" port resource-path))
  ([server port k]
   (key-url (resource-url server port) k)))

(def timeout-ms 5000)

(defn- remote-or-local-cas!
  [chan lease-atom k v seqno]
  (if (lead/am-leader? @lease-atom)
    (do
      (deref (store/set-value! chan k v seqno)
             timeout-ms (f/fail :timeout)))
    (let [remote-url (lead/leader-data @lease-atom)]
      (if remote-url
        (apply client/set-value! (concat [(key-url remote-url k) v]
                                         (if seqno [:seq-no seqno])))
        (f/fail :no-leader)))))

(defn- remote-or-local-get
  [chan lease-atom k]
  (if (lead/am-leader? @lease-atom)
    (deref (store/get-value chan k)
           timeout-ms (f/fail :timeout))
    (let [remote-url (lead/leader-data @lease-atom)]
      (if remote-url
        (client/get-value (key-url remote-url k)) ; client will handle if too old
        (f/fail :no-leader)))))

(defn- create-routes
  [chan lease-atom]
  (routes
   (POST "/registers/:key" request
         (let [k (get-in request [:params :key])
               seqno (get-in request [:body :seq])
               v (get-in request [:body :value])
               result (remote-or-local-cas! chan lease-atom k v seqno)]
           (if (f/failed? result)
             (status (response (str "Failed with: " (f/message result))) 503)
             (response {:updated result}))))
   (GET "/registers/:key" request
        (let [k (get-in request [:params :key])
              result (remote-or-local-get chan lease-atom k)]
          (if (f/failed? result)
            (status (response (str "Failed with: " (f/message result))) 503)
            (response result))))
   (route/not-found "Not Found")))

(defn create-handler
  [chan lease-atom]
  (wrap-json-body
   (wrap-json-response
    (wrap-defaults (create-routes chan lease-atom) api-defaults))
   {:keywords? true :bigdecimals? true}))
