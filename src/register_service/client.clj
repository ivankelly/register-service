(ns register-service.client
  (:require [clj-http.client :as clj-http]
            [failjure.core :as f]))

(def request-defaults {:as :json
                       :throw-exceptions false
                       :socket-timeout 5000
                       :conn-timeout 5000})

(defn get-value [url]
  "Get the value of the register from a remote server"
  (f/attempt-all [response (f/try* (clj-http/get url request-defaults))]
                 (if (= (:status response) 200)
                   (get-in response [:body :value])
                   (f/fail (:body response)))))

(defn check-and-set! [url expected new]
  "Check and set the value of a register on a remote server"
  (f/attempt-all [params (merge request-defaults
                                {:form-params {:expected expected :new new}
                                 :content-type :json})
                  response (f/try* (clj-http/post url params))]
                 (if (= (:status response) 200)
                   (get-in response [:body :updated])
                   (f/fail (:body response)))))