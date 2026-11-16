(ns jepsen.k8s.chaos-mesh.probe
  "Asynchronous probes for observing fault effects."
  (:require [clojure.tools.logging :refer [info warn]])
  (:import (java.util UUID)
           (java.util.concurrent Executors Future ScheduledExecutorService
                                 ThreadFactory TimeUnit)))

(defprotocol Probe
  (sample! [this test context]
    "Takes one fault-effect sample and returns loggable data."))

(def ^:private default-interval-ms 1000)
(def ^:private default-stop-timeout-ms 5000)

(defrecord Runner [probe interval-ms stop-timeout-ms state lock])

(defn runner
  "Creates a reusable asynchronous runner for a Probe."
  ([probe]
   (runner probe {}))
  ([probe {:keys [interval-ms stop-timeout-ms]
           :or {interval-ms default-interval-ms
                stop-timeout-ms default-stop-timeout-ms}}]
   (when-not (satisfies? Probe probe)
     (throw (ex-info "probe must satisfy Probe" {:probe probe})))
   (when-not (and (integer? interval-ms) (pos? interval-ms))
     (throw (ex-info "probe interval-ms must be a positive integer"
                     {:interval-ms interval-ms})))
   (when-not (and (integer? stop-timeout-ms) (pos? stop-timeout-ms))
     (throw (ex-info "probe stop-timeout-ms must be a positive integer"
                     {:stop-timeout-ms stop-timeout-ms})))
   (->Runner probe interval-ms stop-timeout-ms (atom nil) (Object.))))

(defn- active?
  [runner run-id]
  (= run-id (:run-id @(:state runner))))

(defn- thread-factory
  [run-id]
  (reify ThreadFactory
    (newThread [_ task]
      (doto (Thread. ^Runnable task (str "jepsen-k8s-probe-" run-id))
        (.setDaemon true)))))

(defn- stop-entry!
  [runner {:keys [executor task]}]
  (let [^ScheduledExecutorService executor executor
        ^Future task task]
    (when task
      (.cancel task true))
    (.shutdownNow executor)
    (when-not (.awaitTermination executor
                                 (:stop-timeout-ms runner)
                                 TimeUnit/MILLISECONDS)
      (warn "failure probe did not stop before timeout"
            {:timeout-ms (:stop-timeout-ms runner)}))))

(defn stop!
  "Stops the current sampling run. Never throws into the nemesis lifecycle."
  [runner]
  (locking (:lock runner)
    (when-let [entry @(:state runner)]
      (reset! (:state runner) nil)
      (try
        (stop-entry! runner entry)
        (catch Exception e
          (warn "failure probe stop failed (ignored)"
                {:error (.getMessage e)})))))
  runner)

(defn- sample-task
  [runner run-id test context]
  (reify Runnable
    (run [_]
      (try
        (let [measurement (sample! (:probe runner) test context)]
          (when (active? runner run-id)
            (info "failure probe sample"
                  (assoc context
                         :probe-run-id run-id
                         :sampled-at-ms (System/currentTimeMillis)
                         :measurement measurement))))
        (catch InterruptedException _)
        (catch Exception e
          (when (active? runner run-id)
            (warn "failure probe sample failed (ignored)"
                  (assoc context
                         :probe-run-id run-id
                         :sampled-at-ms (System/currentTimeMillis)
                         :error (.getMessage e)))))))))

(defn start!
  "Starts sampling, replacing any current run. Never throws after construction."
  [runner test context]
  (locking (:lock runner)
    (stop! runner)
    (let [run-id   (str (UUID/randomUUID))
          executor (Executors/newSingleThreadScheduledExecutor
                    (thread-factory run-id))
          entry    {:run-id run-id :executor executor}]
      (reset! (:state runner) entry)
      (try
        (let [task (.scheduleWithFixedDelay
                    executor
                    (sample-task runner run-id test context)
                    0
                    (:interval-ms runner)
                    TimeUnit/MILLISECONDS)]
          (swap! (:state runner)
                 #(if (= run-id (:run-id %))
                    (assoc % :task task)
                    %)))
        (catch Exception e
          (reset! (:state runner) nil)
          (.shutdownNow executor)
          (warn "failure probe start failed (ignored)"
                (assoc context :error (.getMessage e)))))))
  runner)
