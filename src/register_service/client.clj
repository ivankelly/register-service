(ns register-service.client
  (:require [clj-http.client :as clj-http]
            [failjure.core :as f]))

(def request-defaults {:as :json
                       :throw-exceptions false
                       :socket-timeout 5000
                       :conn-timeout 5000})

(defn get-value
  "Get the value of the register from a remote server"
  [url & {:keys [seq-no] :or {seq-no 0}}]
  (f/attempt-all [response (f/try* (clj-http/get url request-defaults))]
                 (if (= (:status response) 200)
                   (let [result (:body response)
                         result-seq-no (:seq result)]
                   (if (> seq-no result-seq-no)
                     (f/fail "Server returned an old result")
                     result))
                 (f/fail (:body response)))))

(defn set-value!
  "Set the value of a register on a remote server"
  [url value & {:keys [seq-no]}]
  (f/attempt-all [params (merge request-defaults
                                {:form-params (let [params {:value value}]
                                                (if seq-no
                                                  (assoc params :seq seq-no)
                                                  params))
                                 :content-type :json})
                  response (f/try* (clj-http/post url params))]
                 (if (= (:status response) 200)
                   (get-in response [:body :updated])
                   (f/fail (:body response)))))
