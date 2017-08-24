(ns register-service.app-test
  (:require [clojure.test :refer :all]
            [register-service.app :as app]
            [clj-async-test.core :refer :all]
            [register-service.client :as client]
            [register-service.handler :as handler]
            [register-service.util :as util]
            [bookkeeper.client :as bk])
  (:import [org.apache.zookeeper KeeperException]))

(use-fixtures :each util/bk-fixture)

(defn server-thread [port zk]
  (Thread. (fn []
             (app/app-main (fn [code]
                             (println "Exited with code " code))
                           ["-p" (str port)
                            "-z" zk]))))

(deftest test-app
  (testing "App starts and can be written to"
    (let [port1 3111
          port2 3112
          url1 (handler/resource-url (app/local-ip) port1)
          url2 (handler/resource-url (app/local-ip) port2)
          server1 (server-thread port1 util/*zkconnect*)
          server2 (server-thread port2 util/*zkconnect*)]
      (.start server1)
      (.start server2)
      (is (eventually (= (:value (client/get-value url1 0)) 0)))
      (is (client/set-value! url1 10 0))
      (is (= (:value (client/get-value url2 0)) 10))
      (.interrupt server1)
      (.interrupt server2)
      (.join server1)
      (.join server2))))
