(ns register-service.app
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [register-service.handler :refer [create-handler]]
            [register-service.store :as st]
            [clojure.core.async :refer [>!!]]
            [clojure.tools.cli :refer [parse-opts]]
            [bookkeeper.client :as bk]
            [zookeeper :as zk])
  (:import [java.net DatagramSocket InetAddress])
  (:gen-class))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :default 3111
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 1024 % 0x10000) "Must be a number between 1024 and 65536"]]
   ["-z" "--zookeeper ZOOKEEPER" "ZooKeeper connect string"
    :default "localhost:2181"]])

(defn local-ip []
  (let [socket (DatagramSocket.)]
    (.connect socket (InetAddress/getByName "8.8.8.8") 0)
    (.getHostAddress (.getLocalAddress socket))))

(defn- shutdown-watcher [{:keys [keeper-state]}]
  (if (= keeper-state :Expired)
    (do
      (println "Zookeeper session lost, shutting down")
      (System/exit 1))))

(defn -main
  [& args]
  (let [opts (parse-opts args cli-options)
        connect-string (get-in opts [:options :zookeeper])
        zk (zk/connect connect-string
                       :watcher shutdown-watcher)
        bk (bk/bookkeeper {:zookeeper/connect connect-string})
        error-handler (fn [e]
                        (println "Error in store, quitting. " e)
                        (System/exit 2))
        store (st/init-persistent-store zk bk error-handler)]
    (run-jetty (create-handler store)
               {:port (get-in opts [:options :port])})))
