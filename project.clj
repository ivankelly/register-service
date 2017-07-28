(defproject register-service "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.443"]
                 [compojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [ring/ring-defaults "0.2.1"]
                 [ring/ring-json "0.4.0"]
                 [cheshire "5.7.1"]
                 [org.clojure/tools.cli "0.3.5"]
                 [zookeeper-clj "0.9.4"]
                 [bookkeeper-clj "0.1.0"]
                 [clj-async-test "0.0.5"]]
  :plugins [[lein-ring "0.9.7"]]
  :main register-service.app
  :ring {:handler register-service.handler/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.0"]]}})
