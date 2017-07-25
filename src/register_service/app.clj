(ns register-service.app
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [register-service.handler :refer [create-handler]]
            [register-service.store :as st]
            [clojure.core.async :refer [>!!]])
  (:gen-class))

(defn -main
  [& args]
  (let [chan (st/init-store)]
    (run-jetty (create-handler chan) {:port 3001})))
