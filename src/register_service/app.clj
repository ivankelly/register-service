(ns register-service.app
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [register-service.handler :refer [create-handler]]
            [register-service.store :as st]
            [clojure.core.async :refer [>!!]]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :default 3111
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 1024 % 0x10000) "Must be a number between 1024 and 65536"]]])

(defn -main
  [& args]
  (let [opts (parse-opts args cli-options)
        chan (st/init-store)]
    (run-jetty (create-handler chan)
               {:port (get-in opts [:options :port])})))
