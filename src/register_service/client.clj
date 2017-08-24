(ns register-service.client
  (:require [clj-http.client :as clj-http]
            [failjure.core :as f]))

(def request-defaults {:as :json
                       :throw-exceptions false
                       :socket-timeout 5000
                       :conn-timeout 5000})

(defn get-value [url expected-seqno]
  "Get the value of the register from a remote server"
  (f/attempt-all [response (f/try* (clj-http/get url request-defaults))]
                 (if (= (:status response) 200)
                   (let [result (:body response)
                         seqno (:seq result)]
                     (if (> expected-seqno seqno)
                       (f/fail "Server returned an old result")
                       result))
                   (f/fail (:body response)))))

(defn set-value!
  "Set the value of a register on a remote server"
  ([url value]
   (set-value! url value nil))
  ([url value seqno]
   (f/attempt-all [params (merge request-defaults
                                 {:form-params (let [params {:value value}]
                                                 (if seqno
                                                   (assoc params :seq seqno)
                                                   params))
                                  :content-type :json})
                   response (f/try* (clj-http/post url params))]
                  (if (= (:status response) 200)
                    (get-in response [:body :updated])
                    (f/fail (:body response))))))
