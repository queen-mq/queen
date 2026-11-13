(ns jepsen.queen.http
  "A small JSON-over-HTTP/1.1 client for Queen, on java.net.http. One
  HttpClient per Jepsen client, so a crashed client drops its connections.

  `request!` never retries. It returns {:status s, :body parsed-json}, or throws
  ex-info whose :type says whether the request can have reached the server:

    ::refused  the TCP connection was never established (definite: not sent)
    ::timeout  no answer within the request timeout (indefinite)
    ::io       the connection broke mid-request (indefinite)"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jepsen.control.net :as cn])
  (:import (java.io IOException)
           (java.net ConnectException URI)
           (java.net.http HttpClient
                          HttpClient$Version
                          HttpConnectTimeoutException
                          HttpRequest
                          HttpRequest$Builder
                          HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers
                          HttpTimeoutException)
           (java.time Duration)))

(def http-port
  "The broker's HTTP port on every node: 6632. QUEEN_JEPSEN_HTTP_PORT moves it,
  for a smoke run of a client against a scratch broker on a laptop."
  (or (some-> (System/getenv "QUEEN_JEPSEN_HTTP_PORT") parse-long)
      6632))

(defonce ^{:doc "The port clients use: http-port, or the embedded proxy's port with
  --via-proxy (set by jepsen.queen.core when it builds the test)."}
  client-port
  (atom http-port))

(defonce ^{:doc "Headers every request to the client port carries (--via-proxy: the
  API key)."}
  client-headers
  (atom {}))

(defn base-url
  "The URL clients use on a node: http://<private ip>:<client port>. The
  bare broker unless --via-proxy, then the embedded proxy."
  [node]
  (str "http://" (cn/ip node) ":" @client-port))

(defn broker-url
  "http://<private ip>:6632, the bare broker, whatever clients use: for
  health, membership, link and metrics calls the harness makes itself."
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
  "Sends one request (method :get, :post, :put or :delete; body a Clojure
  value encoded as JSON, or nil), with the `headers` map's name/value pairs
  when given. See the namespace doc for the result and the exceptions."
  ([c method url body timeout-ms]
   (request! c method url body timeout-ms nil))
  ([^HttpClient c method url body timeout-ms headers]
  (let [b (-> (HttpRequest/newBuilder (URI/create url))
              (.timeout (Duration/ofMillis (long timeout-ms)))
              (.header "Content-Type" "application/json"))
        ; The client port's own headers (the proxy's API key) go only to
        ; the client port: the bare broker never sees a credential.
        dflt (when (and (not= @client-port http-port)
                        (str/includes? (str url) (str ":" @client-port "/")))
               @client-headers)
        b (reduce-kv (fn [^HttpRequest$Builder b k v] (.header b (str k) (str v))) b
                     (merge dflt headers))
        b (case method
            :get    (.GET b)
            :delete (.DELETE b)
            :post   (.POST b (HttpRequest$BodyPublishers/ofString
                               (json/write-str body)))
            :put    (.PUT b (HttpRequest$BodyPublishers/ofString
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
          (throw (ex-info "io error" {:type ::io, :msg (str e)}))))))))

(defn health
  "GET /health of a node: the parsed body (also on 503 settling), or nil."
  [^HttpClient c node timeout-ms]
  (try (:body (request! c :get (str (broker-url node) "/health") nil timeout-ms))
       (catch clojure.lang.ExceptionInfo _ nil)))
