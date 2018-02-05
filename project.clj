(defproject register-service "0.2.1"
  :description "A simple service that exposes a single register backed by bookkeeper"
  :url "http://github.com/ivankelly/register-service"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.443"]
                 [compojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [ring/ring-defaults "0.2.1"]
                 [ring/ring-json "0.4.0"]
                 [cheshire "5.7.1"]
                 [failjure "1.0.1"]
                 [org.clojure/tools.cli "0.3.5"]
                 [zookeeper-clj "0.9.4"]
                 [bookkeeper-clj "0.1.3"]
                 [clj-async-test "0.0.5"]
                 [clj-http "3.6.1"]]
  :plugins [[lein-ring "0.9.7"]]
  :main register-service.app
  :ring {:handler register-service.handler/app}
  :deploy-repositories [["releases" :clojars]]
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.0"]]}})
