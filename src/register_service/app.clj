(ns register-service.app
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [register-service.handler :refer [create-handler resource-url]]
            [register-service.leadership :as leadership]
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
    :default "localhost:2181"]
   ["-t" "--zookeeper-timeout-ms TIMEOUT" "ZooKeeper session timeout"
    :parse-fn #(Integer/parseInt %)
    :default 5000]])

(defn local-ip []
  (let [socket (DatagramSocket.)]
    (.connect socket (InetAddress/getByName "8.8.8.8") 0)
    (.getHostAddress (.getLocalAddress socket))))

(defn- shutdown-watcher [exit-fn]
  (fn [{:keys [keeper-state]}]
    (if (= keeper-state :Expired)
      (do
        (println "Zookeeper session lost, shutting down")
        (exit-fn 1)))))

(defn app-main
  [exit-fn args]
  (let [opts (parse-opts args cli-options)
        connect-string (get-in opts [:options :zookeeper])
        zk (zk/connect connect-string
                       :timeout-msec (get-in opts [:options
                                                   :zookeeper-timeout-ms])
                       :watcher (shutdown-watcher exit-fn))
        bk (bk/bookkeeper {:zookeeper/connect connect-string})
        error-handler (fn [e]
                        (println "Error in store, quitting. " e)
                        (exit-fn 2))
        store (st/init-persistent-store zk bk error-handler)
        port (get-in opts [:options :port])
        url (resource-url (local-ip) port)
        lease (leadership/join-group zk url
                                     (fn [] (st/become-leader! store))
                                     (fn []
                                       (println "Lost leadership, quitting")
                                       (exit-fn 4)))]
    (try
      (run-jetty (create-handler store (atom lease))
                 {:port port})
      (catch Exception e
        (println "Caught exception " e)
        (exit-fn 3)))))

(defn -main
  [& args]
  (app-main (fn [code]
              (System/exit code)) args))
