(ns jepsen.k8s.chaos-mesh.stress
  "CPU and memory load using Chaos Mesh's StressChaos."
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [jepsen.generator :as gen]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as jn]))

(def ^:private experiment-name "stress-fault")
(def ^:private target-specs
  #{:one :minority :majority :minority-third :all})

(defn- valid-target?
  "Is t one element of :targets, i.e. one whole spec an op can be given?"
  [t]
  (or (nil? t)
      (contains? target-specs t)
      (and (sequential? t)
           (seq t)
           (every? #(and (string? %) (not (str/blank? %))) t))))

(defn- validate-targets
  [targets]
  (when-not (and (sequential? targets) (seq targets))
    (throw (ex-info "targets must be a non-empty collection"
                    {:targets targets})))
  (when-let [invalid (seq (remove valid-target? targets))]
    (throw (ex-info (str "each element of targets must be nil, :one, "
                         ":minority, :majority, :minority-third, :all, or a "
                         "collection of pod names; note that a list of pod "
                         "names has to be nested, as in "
                         "[[\"pod-0\" \"pod-1\"]], because each op uses one "
                         "element of targets as its whole spec")
                    {:targets targets
                     :invalid (vec invalid)}))))

(defn- validate-cpu
  [cpu]
  (when-not (map? cpu)
    (throw (ex-info "cpu must be a map" {:cpu cpu})))
  (let [{:keys [workers load] :as cpu} (merge {:workers 1 :load 100} cpu)]
    (when-not (and (integer? workers) (pos? workers))
      (throw (ex-info "cpu workers must be a positive integer"
                      {:workers workers})))
    (when-not (and (integer? load) (<= 0 load 100))
      (throw (ex-info "cpu load must be an integer from 0 through 100"
                      {:load load})))
    cpu))

(defn- validate-memory
  [memory]
  (when-not (map? memory)
    (throw (ex-info "memory must be a map" {:memory memory})))
  (let [{:keys [workers size time oom-score-adj] :as memory}
        (merge {:workers 1} memory)]
    (when-not (and (integer? workers) (pos? workers))
      (throw (ex-info "memory workers must be a positive integer"
                      {:workers workers})))
    (when-not (and (string? size) (not (str/blank? size)))
      (throw (ex-info "memory size must be a non-empty string"
                      {:size size})))
    (when (some? time)
      (when-not (and (string? time) (not (str/blank? time)))
        (throw (ex-info "memory time must be a non-empty string"
                        {:time time}))))
    (when (some? oom-score-adj)
      (when-not (and (integer? oom-score-adj)
                     (<= -1000 oom-score-adj 1000))
        (throw (ex-info (str "memory oom-score-adj must be an integer from "
                             "-1000 through 1000")
                        {:oom-score-adj oom-score-adj}))))
    memory))

(defn- validate-config
  [config]
  (when-not (map? config)
    (throw (ex-info "stress configuration must be a map"
                    {:stress config})))
  (when-not (or (some? (:cpu config)) (some? (:memory config)))
    (throw (ex-info "stress configuration requires :cpu, :memory, or both"
                    {:stress config})))
  (let [config (cond-> (merge {:targets [:one]} config)
                 (some? (:cpu config))
                 (update :cpu validate-cpu)

                 (some? (:memory config))
                 (update :memory validate-memory))]
    (validate-targets (:targets config))
    (when-let [container-names (:container-names config)]
      (when-not (and (sequential? container-names)
                     (seq container-names)
                     (every? #(and (string? %) (not (str/blank? %)))
                             container-names))
        (throw (ex-info "container-names must be a non-empty collection of names"
                        {:container-names container-names}))))
    ;; Validate this early instead of failing during pod discovery.
    (k8s/label-selector (:pod-selector config))
    config))

(defn- memory-stressor
  [{:keys [workers size time oom-score-adj]}]
  (cond-> {:workers workers
           :size size}
    time (assoc :time time)
    (some? oom-score-adj) (assoc :oomScoreAdj oom-score-adj)))

(defn- cpu-stressor
  [{:keys [workers load]}]
  {:workers workers
   :load load})

(defn- make-manifest
  [test targets {:keys [cpu memory container-names]}]
  (yaml/generate-string
   {:apiVersion "chaos-mesh.org/v1alpha1"
    :kind "StressChaos"
    :metadata {:name experiment-name
               :namespace "chaos-mesh"}
    :spec (cond-> {:mode "all"
                   :selector {:pods {(k8s/namespace test) (vec targets)}}
                   :stressors (cond-> {}
                                cpu (assoc :cpu (cpu-stressor cpu))
                                memory (assoc :memory
                                              (memory-stressor memory)))}
            container-names (assoc :containerNames (vec container-names)))}))

(defn- stop!
  [test]
  (exp/stop! test {:name experiment-name :kind "stresschaos"})
  :stress-healed)

(defn- apply!
  [test targets config dir]
  (stop! test)
  (try
    (exp/apply! test (make-manifest test targets config) dir)
    {:fault-kind :stress
     :targets (vec targets)
     :stressors (select-keys config [:cpu :memory])}
    (catch Exception e
      (stop! test)
      (throw e))))

(defn- stress-nemesis
  [config dir]
  (reify
    n/Reflection
    (fs [_this] [:start-stress :stop-stress])

    n/Nemesis
    (setup! [this test]
      ;; The package always builds this nemesis, even when :stress was not
      ;; requested. Only a configured stress fault should touch the cluster.
      (when config
        (stop! test))
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result
            (case f
              :start-stress
              (let [eligible (k8s/pod-names
                              test
                              {:selector (:pod-selector config)})
                    targets  (exp/select-targets eligible value)]
                (when (empty? targets)
                  (throw (ex-info "stress fault selected no eligible pods"
                                  {:pod-selector (:pod-selector config)
                                   :target-spec value
                                   :eligible-pods eligible})))
                (apply! test targets config dir))

              :stop-stress
              (stop! test))]
        (assoc op :value result)))

    (teardown! [_this test]
      (when config
        (stop! test)))))

(defn stress-package
  "Builds a package that applies CPU load, memory pressure, or both.

  Options under :stress:

    :cpu              CPU stressor. Optional keys are :workers (positive
                      integer, default 1) and :load (0-100, default 100).
    :memory           Memory stressor. :size is required, e.g. 256MB or 25%.
                      Optional keys are :workers (positive integer,
                      default 1), :time (linear ramp duration), and
                      :oom-score-adj (-1000 through 1000).
    :pod-selector     Label map choosing eligible pods. Defaults to every pod
                      in the test namespace.
    :container-names  Containers within those pods. Defaults to all.
    :targets          Specs to pick from, one per op. Each element is nil,
                      :one, :minority, :majority, :minority-third, :all, or a
                      nested collection of pod names. Defaults to [:one].

  At least one of :cpu or :memory is required. Supplying both applies both
  stressors to the same selected pods."
  [opts]
  (let [needed? (contains? (:faults opts) :stress)
        config  (when needed? (validate-config (:stress opts)))
        targets (:targets config)
        start   (fn [_test _context]
                  {:type :info
                   :f :start-stress
                   :value (rand-nth targets)})
        stop    {:type :info :f :stop-stress :value nil}
        gen     (when needed?
                  (->> (gen/flip-flop start (gen/repeat stop))
                       (gen/stagger (:interval opts jn/default-interval))))]
    {:generator gen
     :final-generator (when needed? stop)
     :nemesis (stress-nemesis config (:dir opts))
     :perf #{{:name "stress"
              :start #{:start-stress}
              :stop #{:stop-stress}
              :color "#E9A23B"}}}))
