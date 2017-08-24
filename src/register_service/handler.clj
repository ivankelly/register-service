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

(def resource-path "/register")

(defn resource-url [server port]
  (str "http://" server ":" port resource-path))

(def timeout-ms 5000)

(defn- remote-or-local-cas!
  [chan lease-atom value seqno]
  (if (lead/am-leader? @lease-atom)
    (do
      (deref (store/set-value! chan value seqno)
             timeout-ms (f/fail :timeout)))
    (let [remote-url (lead/leader-data @lease-atom)]
      (if remote-url
        (apply client/set-value! (concat [remote-url value]
                                         (if seqno [:seq-no seqno])))
        (f/fail :no-leader)))))

(defn- remote-or-local-get
  [chan lease-atom]
  (if (lead/am-leader? @lease-atom)
    (deref (store/get-value chan)
           timeout-ms (f/fail :timeout))
    (let [remote-url (lead/leader-data @lease-atom)]
      (if remote-url
        (client/get-value remote-url) ; client will handle if too old
        (f/fail :no-leader)))))

(defn- create-routes
  [chan lease-atom]
  (routes
   (POST "/register" request
         (let [seqno (get-in request [:body :seq])
               value (get-in request [:body :value])
               result (remote-or-local-cas! chan lease-atom value seqno)]
           (if (f/failed? result)
             (status (response (str "Failed with: " (f/message result))) 503)
             (response {:updated result}))))
   (GET "/register" []
        (let [result (remote-or-local-get chan lease-atom)]
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
