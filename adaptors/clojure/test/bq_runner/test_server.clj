(ns bq-runner.test-server
  "Singleton test server that starts bq-runner on first access and cleans up on JVM shutdown."
  (:require [clojure.java.io :as io])
  (:import [java.lang ProcessBuilder ProcessBuilder$Redirect]
           [java.net Socket]
           [java.io File]))

(def ^:private server-port 3000)
(def ^:private project-root
  (let [cwd (System/getProperty "user.dir")]
    (if (.exists (io/file cwd "Cargo.toml"))
      cwd
      (-> (io/file cwd) .getParentFile .getParentFile .getAbsolutePath))))

(def ^:private binary-path
  (str project-root "/target/release/bq-runner"))

(defn- port-available? [port]
  (try
    (let [socket (Socket. "localhost" port)]
      (.close socket)
      false)
    (catch Exception _ true)))

(defn- wait-for-server [timeout-ms]
  (let [start (System/currentTimeMillis)]
    (loop []
      (cond
        (not (port-available? server-port))
        true

        (> (- (System/currentTimeMillis) start) timeout-ms)
        false

        :else
        (do (Thread/sleep 100)
            (recur))))))

(defn- build-server []
  (println "Building bq-runner server...")
  (let [pb (ProcessBuilder. ["cargo" "build" "--release"])
        _ (.directory pb (File. project-root))
        _ (.redirectErrorStream pb true)
        proc (.start pb)
        exit-code (.waitFor proc)]
    (when-not (zero? exit-code)
      (throw (ex-info "Failed to build server"
                      {:exit-code exit-code
                       :output (slurp (.getInputStream proc))})))
    (println "  Build complete")))

(defn- start-server-process []
  (when-not (.exists (File. binary-path))
    (build-server))
  (println "Starting bq-runner server...")
  (let [pb (ProcessBuilder. [binary-path])
        _ (.directory pb (File. project-root))
        _ (.redirectOutput pb ProcessBuilder$Redirect/INHERIT)
        _ (.redirectError pb ProcessBuilder$Redirect/INHERIT)
        proc (.start pb)]
    (if (wait-for-server 10000)
      (do (println "  Server ready on port" server-port)
          proc)
      (do (.destroyForcibly proc)
          (throw (ex-info "Server failed to start within timeout" {}))))))

(defonce ^:private server-process
  (delay
    (let [proc (start-server-process)]
      (.addShutdownHook
        (Runtime/getRuntime)
        (Thread.
          (fn []
            (println "\nShutting down test server...")
            (.destroyForcibly proc)
            (.waitFor proc 5 java.util.concurrent.TimeUnit/SECONDS))))
      proc)))

(defn ensure-server!
  "Ensures the test server is running. Call this before running tests.
   Returns the WebSocket URL."
  []
  @server-process
  (str "ws://localhost:" server-port "/ws"))

(defn server-url []
  (str "ws://localhost:" server-port "/ws"))
