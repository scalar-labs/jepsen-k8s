(ns jepsen.k8s.chaos-mesh.file-io
  "File read/write failures using Chaos Mesh's IOChaos."
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [jepsen.generator :as gen]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as jn])
  (:import (java.nio.file Path Paths)))

(def ^:private experiment-name "file-io-fault")
(def ^:private io-methods #{:read :write})
(def ^:private required-arch "amd64")
(def ^:private target-specs
  #{:one :minority :majority :minority-third :all})

(defn- valid-target?
  "Is t one element of :targets, i.e. one whole spec an op can be given?

  Each op picks one element of :targets at random and hands that element to
  select-targets, so a list of pod names has to be nested to survive the pick:
  [[\"pod-0\" \"pod-1\"]], not [\"pod-0\" \"pod-1\"]."
  [t]
  (or (nil? t)
      (contains? target-specs t)
      (and (sequential? t)
           (seq t)
           (every? #(and (string? %) (not (str/blank? %))) t))))

(defn- check-node-arch!
  "Throws unless every node in the cluster is amd64.

  IOChaos injects `toda`, whose binary is x86-64 even in the arm64 chaos-daemon
  image, so on any other architecture it dies under emulation and no fault is
  ever applied."
  [test]
  (let [archs (->> (k8s/nodes test)
                   :items
                   (mapv (juxt #(get-in % [:metadata :name])
                               #(get-in % [:status :nodeInfo :architecture]))))
        wrong (remove (comp #{required-arch} second) archs)]
    (when (empty? archs)
      (throw (ex-info "the file I/O fault found no nodes to check the architecture of"
                      {:required-arch required-arch})))
    (when (seq wrong)
      (throw (ex-info (str "the file I/O fault needs every node on "
                           required-arch
                           "; Chaos Mesh's toda binary is x86-64 even in the "
                           "arm64 chaos-daemon image, so no fault would inject")
                      {:required-arch required-arch
                       :nodes (into {} archs)
                       :unsupported-nodes (into {} wrong)})))))

(defn- path
  [option value]
  (when-not (and (string? value) (not (str/blank? value)))
    (throw (ex-info (str (name option) " must be a non-empty string")
                    {:option option :value value})))
  (let [path (Paths/get value (make-array String 0))]
    (when-not (.isAbsolute path)
      (throw (ex-info (str (name option) " must be absolute")
                      {:option option :value value})))
    (.normalize path)))

(defn- validate-config
  [config]
  (let [config      (merge {:targets [:one]
                            :methods [:read :write]
                            :errno 5
                            :percent 100}
                           config)
        volume-path (path :volume-path (:volume-path config))
        file-path   (path :file-path (:file-path config))
        methods     (:methods config)
        targets     (:targets config)
        errno       (:errno config)
        percent     (:percent config)]
    (when-not (.startsWith ^Path file-path ^Path volume-path)
      (throw (ex-info "file-path must be within volume-path"
                      {:volume-path (:volume-path config)
                       :file-path (:file-path config)})))
    (when-not (and (sequential? methods)
                   (seq methods)
                   (every? io-methods methods))
      (throw (ex-info "methods must contain :read, :write, or both"
                      {:methods methods})))
    (when-not (and (sequential? targets) (seq targets))
      (throw (ex-info "targets must be a non-empty collection"
                      {:targets targets})))
    ;; A bare pod name here passes every shape check and then throws on the
    ;; first op, which is exactly what this function exists to prevent.
    (when-let [invalid (seq (remove valid-target? targets))]
      (throw (ex-info (str "each element of targets must be nil, :one, "
                           ":minority, :majority, :minority-third, :all, or a "
                           "collection of pod names; note that a list of pod "
                           "names has to be nested, as in "
                           "[[\"pod-0\" \"pod-1\"]], because each op uses one "
                           "element of targets as its whole spec")
                      {:targets targets
                       :invalid (vec invalid)})))
    (when-not (and (integer? errno) (pos? errno))
      (throw (ex-info "errno must be a positive integer" {:errno errno})))
    (when-not (and (integer? percent) (<= 0 percent 100))
      (throw (ex-info "percent must be an integer from 0 through 100"
                      {:percent percent})))
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

(defn- make-manifest
  [test targets {:keys [volume-path file-path methods errno percent
                        container-names]}]
  (yaml/generate-string
   {:apiVersion "chaos-mesh.org/v1alpha1"
    :kind "IOChaos"
    :metadata {:name experiment-name
               :namespace "chaos-mesh"}
    :spec (cond-> {:action "fault"
                   :mode "all"
                   :selector {:pods {(k8s/namespace test) (vec targets)}}
                   :volumePath volume-path
                   :path file-path
                   :methods (mapv (comp str/upper-case name) methods)
                   :errno errno
                   :percent percent}
            container-names (assoc :containerNames (vec container-names)))}))

(defn- stop!
  [test]
  (exp/stop! test {:name experiment-name :kind "iochaos"})
  :file-io-healed)

(defn- apply!
  [test targets config dir]
  (stop! test)
  (try
    (exp/apply! test (make-manifest test targets config) dir)
    {:fault-kind :file-io
     :targets (vec targets)
     :volume-path (:volume-path config)
     :file-path (:file-path config)
     :methods (:methods config)
     :errno (:errno config)
     :percent (:percent config)}
    (catch Exception e
      (stop! test)
      (throw e))))

(defn- file-io-nemesis
  [config dir]
  (reify
    n/Reflection
    (fs [_this] [:start-file-io :stop-file-io])

    n/Nemesis
    (setup! [this test]
      ;; Sweep even when this run didn't select :file-io, like the other kinds:
      ;; experiment-name is a fixed literal, so a leftover re-attaches under a
      ;; later run and gets blamed on whichever nemesis that one selected. It
      ;; has to precede the arch check: that check throws on an unreadable
      ;; cluster as readily as on a wrong architecture, and a throw there means
      ;; teardown! never runs.
      (stop! test)
      (when config
        (check-node-arch! test))
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result
            (case f
              :start-file-io
              (let [eligible (k8s/pod-names
                              test
                              {:selector (:pod-selector config)})
                    targets  (exp/select-targets eligible value)]
                (when (empty? targets)
                  (throw (ex-info "file I/O fault selected no eligible pods"
                                  {:pod-selector (:pod-selector config)
                                   :target-spec value
                                   :eligible-pods eligible})))
                (apply! test targets config dir))

              :stop-file-io
              (stop! test))]
        (assoc op :value result)))

    (teardown! [_this test]
      (stop! test))))

(defn file-io-package
  "Builds a package that makes READ and/or WRITE calls return an errno.

  Required options under :file-io:

    :volume-path      Absolute path of the mounted volume to fault, e.g.
                      \"/var/lib/postgresql/data\".
    :file-path        Absolute path, or glob, within :volume-path to fault,
                      e.g. \"/var/lib/postgresql/data/pg_wal/**/*\".

  Optional:

    :pod-selector     Label map choosing which pods are eligible, e.g.
                      {:app \"postgres\"}. Defaults to every pod in the test
                      namespace, which is the fault's whole blast radius, so
                      it is worth setting.
    :container-names  Containers within those pods to fault. Defaults to all.
    :methods          Any of [:read :write]. Defaults to both.
    :errno            Positive errno the faulted calls return. Defaults to 5,
                      EIO.
    :percent          Percentage of matching calls to fail, 0-100. Defaults
                      to 100.
    :targets          Specs to pick from, one per op. Each element is nil,
                      :one, :minority, :majority, :minority-third, :all, or a
                      nested collection of pod names such as
                      [[\"pod-0\" \"pod-1\"]]. Defaults to [:one].

  Every node must be amd64; setup! throws otherwise. See check-node-arch!."
  [opts]
  (let [needed? (contains? (:faults opts) :file-io)
        config  (when needed? (validate-config (:file-io opts)))
        targets (:targets config)
        start   (fn [_test _context]
                  {:type :info
                   :f :start-file-io
                   :value (rand-nth targets)})
        stop    {:type :info :f :stop-file-io :value nil}
        gen     (when needed?
                  (->> (gen/flip-flop start (gen/repeat stop))
                       (gen/stagger (:interval opts jn/default-interval))))]
    {:generator gen
     :final-generator (when needed? stop)
     :nemesis (file-io-nemesis config (:dir opts))
     :perf #{{:name "file-io"
              :start #{:start-file-io}
              :stop #{:stop-file-io}
              :color "#E6A8D7"}}}))
