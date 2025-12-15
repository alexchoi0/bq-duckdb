(ns bq-runner.client
  "WebSocket connection management for bq-runner using Java 11+ interop"
  (:require [clojure.data.json :as json])
  (:import [java.net URI]
           [java.net.http HttpClient WebSocket WebSocket$Listener]
           [java.util.concurrent LinkedBlockingQueue TimeUnit CompletableFuture]
           [java.nio CharBuffer]))

(defrecord Connection [^WebSocket ws pending-responses closed? message-buffer])

(defn- generate-id []
  (str (java.util.UUID/randomUUID)))

(defn- make-request [method params]
  {:jsonrpc "2.0"
   :method method
   :params params
   :id (generate-id)})

(defn- handle-message [conn msg]
  (let [pending (:pending-responses conn)
        response (json/read-str msg :key-fn keyword)
        id (:id response)]
    (when-let [queue (get @pending id)]
      (.put ^LinkedBlockingQueue queue response)
      (swap! pending dissoc id))))

(defn connect
  "Connect to bq-runner WebSocket server.
   Returns a Connection record."
  [url]
  (let [pending (atom {})
        closed? (atom false)
        message-buffer (atom (StringBuilder.))
        conn-atom (atom nil)
        connected-future (CompletableFuture.)
        client (HttpClient/newHttpClient)
        listener (reify WebSocket$Listener
                   (onOpen [_ ws]
                     (.request ws 1)
                     (.complete connected-future true))
                   (onText [_ ws data last?]
                     (.append ^StringBuilder @message-buffer (.toString data))
                     (when last?
                       (let [msg (.toString ^StringBuilder @message-buffer)]
                         (.setLength ^StringBuilder @message-buffer 0)
                         (when-let [c @conn-atom]
                           (handle-message c msg))))
                     (.request ws 1)
                     nil)
                   (onClose [_ ws status reason]
                     (reset! closed? true)
                     nil)
                   (onError [_ ws error]
                     (reset! closed? true)
                     (when-not (.isDone connected-future)
                       (.completeExceptionally connected-future error))
                     nil))
        ws-future (-> client
                      (.newWebSocketBuilder)
                      (.buildAsync (URI. url) listener))]
    (let [ws (.get ws-future 10 TimeUnit/SECONDS)]
      (.get connected-future 5 TimeUnit/SECONDS)
      (let [conn (->Connection ws pending closed? message-buffer)]
        (reset! conn-atom conn)
        conn))))

(defn close
  "Close the WebSocket connection."
  [conn]
  (reset! (:closed? conn) true)
  (.sendClose ^WebSocket (:ws conn) WebSocket/NORMAL_CLOSURE ""))

(defn connected?
  "Check if connection is still open."
  [conn]
  (not @(:closed? conn)))

(defn send-request
  "Send a JSON-RPC request and wait for response.
   Returns the result or throws on error."
  ([conn method params]
   (send-request conn method params 30000))
  ([conn method params timeout-ms]
   (when @(:closed? conn)
     (throw (ex-info "Connection is closed" {:method method})))
   (let [request (make-request method params)
         id (:id request)
         queue (LinkedBlockingQueue. 1)]
     (swap! (:pending-responses conn) assoc id queue)
     (try
       (-> ^WebSocket (:ws conn)
           (.sendText (json/write-str request) true)
           (.get 5 TimeUnit/SECONDS))
       (if-let [response (.poll queue timeout-ms TimeUnit/MILLISECONDS)]
         (if-let [error (:error response)]
           (throw (ex-info (:message error)
                           {:code (:code error)
                            :method method
                            :params params}))
           (:result response))
         (throw (ex-info "Request timed out"
                         {:method method
                          :timeout-ms timeout-ms})))
       (finally
         (swap! (:pending-responses conn) dissoc id))))))
