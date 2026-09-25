(ns jepsen.queen.http
  "A small JSON-over-HTTP/1.1 client for Queen, on java.net.http. One
  HttpClient per Jepsen client, so a crashed client drops its connections.

  `request!` never retries. It returns {:status s, :body parsed-json}, or throws
  ex-info whose :type says whether the request can have reached the server:

    ::refused  the TCP connection was never established (definite: not sent)
    ::timeout  no answer within the request timeout (indefinite)
    ::io       the connection broke mid-request (indefinite)"
  (:require [clojure.data.json :as json]
            [jepsen.control.net :as cn])
  (:import (java.io IOException)
           (java.net ConnectException URI)
           (java.net.http HttpClient
                          HttpClient$Version
                          HttpConnectTimeoutException
                          HttpRequest
                          HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers
                          HttpTimeoutException)
           (java.time Duration)))

(def http-port 6632)

(defn base-url
  "http://<private ip>:6632 for a node name (resolved on the control node)."
  [node]
  (str "http://" (cn/ip node) ":" http-port))

(defn client
  "A fresh HTTP/1.1 client with its own connection pool."
  ^HttpClient []
  (-> (HttpClient/newBuilder)
      (.version HttpClient$Version/HTTP_1_1)
      (.connectTimeout (Duration/ofMillis 2000))
      (.build)))

(defn- parse-body
  [^String s]
  (if (or (nil? s) (= "" s))
    nil
    (try (json/read-str s :key-fn keyword)
         (catch Exception _ s))))

(defn- root-cause
  [^Throwable t]
  (loop [t t]
    (if-let [c (.getCause t)]
      (if (identical? c t) t (recur c))
      t)))

(defn request!
  "Sends one request (method :get or :post, body a Clojure value encoded as
  JSON, or nil). See the namespace doc for the result and the exceptions."
  [^HttpClient c method url body timeout-ms]
  (let [b (-> (HttpRequest/newBuilder (URI/create url))
              (.timeout (Duration/ofMillis (long timeout-ms)))
              (.header "Content-Type" "application/json"))
        b (case method
            :get  (.GET b)
            :post (.POST b (HttpRequest$BodyPublishers/ofString
                             (json/write-str body))))
        req (.build b)]
    (try
      (let [resp (.send c req (HttpResponse$BodyHandlers/ofString))]
        {:status (.statusCode resp)
         :body   (parse-body (.body resp))})
      (catch HttpConnectTimeoutException e
        (throw (ex-info "connect timeout" {:type ::refused, :msg (str e)})))
      (catch HttpTimeoutException e
        (throw (ex-info "request timeout" {:type ::timeout, :msg (str e)})))
      (catch ConnectException e
        (throw (ex-info "connection refused" {:type ::refused, :msg (str e)})))
      (catch IOException e
        (if (instance? ConnectException (root-cause e))
          (throw (ex-info "connection refused" {:type ::refused
                                                :msg (str (root-cause e))}))
          (throw (ex-info "io error" {:type ::io, :msg (str e)})))))))

(defn health
  "GET /health of a node: the parsed body (also on 503 settling), or nil."
  [^HttpClient c node timeout-ms]
  (try (:body (request! c :get (str (base-url node) "/health") nil timeout-ms))
       (catch clojure.lang.ExceptionInfo _ nil)))
