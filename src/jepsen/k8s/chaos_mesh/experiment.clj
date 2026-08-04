(ns jepsen.k8s.chaos-mesh.experiment
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.util :as util]))

(def ^:private ^:const MAX_RETRIES 5)
(def ^:private ^:const SLEEP_MS 1000)

(defn- get-new-path
  [dir]
  (str dir
       "/chaos-mesh-experiment-"
       (System/currentTimeMillis)
       ".yaml"))

(defn apply!
  [test manifest dir]
  (let [path (get-new-path dir)]
    (spit path manifest)
    (k8s/kubectl! test :apply :-f path)))

(defn- fault-exists?
  [test name kind]
  (try
    (k8s/kubectl! test :get kind name
                  :-n "chaos-mesh"
                  :-o :name
                  :--request-timeout "10s")
    true
    (catch Exception _ false)))

(defn- try-force-delete-fault
  [test name kind]
  (loop [i 0]
    (when (< i MAX_RETRIES)
      (when (fault-exists? test name kind)
        (try
          (k8s/kubectl! test :patch kind name
                        :--type :merge
                        :-p "{\"metadata\":{\"finalizers\":[]}}"
                        :-n "chaos-mesh"
                        :--request-timeout "10s")
          (catch Exception e
            (warn kind "finalizer patch failed (ignored)"
                  {:name name :kind kind :retry i :error (.getMessage e)})))

        (try
          (k8s/kubectl! test :delete kind name
                        :-n "chaos-mesh"
                        :--wait=false
                        :--ignore-not-found=true
                        :--request-timeout "10s")
          (catch Exception e
            (warn kind "delete retry failed (ignored)"
                  {:name name :kind kind :retry i :error (.getMessage e)})))

        (Thread/sleep SLEEP_MS)

        (recur (inc i))))))

(defn stop!
  [test {:keys [name kind] :as summary}]
  (when (fault-exists? test name kind)
    (try
      (k8s/kubectl! test :delete kind name
                    :-n "chaos-mesh"
                    :--wait=false
                    :--ignore-not-found=true
                    :--request-timeout "10s")
      (catch Exception e
        (warn kind "delete failed (ignored)"
              {:name name :error (.getMessage e)})))

    ;; if the chaos experiment still exists,
    ;; try to force delete it by removing finalizers for several times
    (try-force-delete-fault test name kind)

    (when (fault-exists? test name kind)
      (warn "experiment still exists after best-effort cleanup"
            {:name name :kind kind}))

    summary))

(defn choose-targets
  [test limit]
  (->> (k8s/pod-names test {})
       shuffle
       (take (inc (rand-int limit)))))

(defn select-targets
  "Selects live pods according to a Jepsen node specification. Explicit pod
  collections are restricted to the supplied eligible pods."
  [pods target-spec]
  (let [pods (vec pods)]
    (if (empty? pods)
      []
      (case target-spec
        nil             (util/random-nonempty-subset pods)
        :one            [(rand-nth pods)]
        :minority       (take (dec (util/majority (count pods))) (shuffle pods))
        :majority       (take (util/majority (count pods)) (shuffle pods))
        :minority-third (take (util/minority-third (count pods)) (shuffle pods))
        :all            pods
        (if (sequential? target-spec)
          (filterv (set pods) target-spec)
          (throw (ex-info "unknown target spec" {:spec target-spec})))))))
