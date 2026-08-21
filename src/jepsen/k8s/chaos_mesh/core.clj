(ns jepsen.k8s.chaos-mesh.core
  "Composing Jepsen nemeses with k8s/Chaos Mesh backends."
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.k8s.chaos-mesh
             [clock :as clock]
             [file-io :as file-io]
             [network :as network]
             [pod :as pod]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [jepsen.nemesis.combined :as jn]
            [jepsen.net :as net]))

(def default-namespace "chaos-mesh")
(def default-release "chaos-mesh")
(def default-repo-name "chaos-mesh")
(def default-repo-url "https://charts.chaos-mesh.org")
(def default-chart "chaos-mesh/chaos-mesh")
(def default-wipe-timeout "300s")
(def default-version "2.8.2")

(def runtime-values
  "Chaos Mesh's chaos-daemon talks to the container runtime to resolve a
  container into the PID whose namespaces it enters. Every fault but pod chaos
  needs that, so a wrong runtime leaves the daemon unable to inject anything.

  Keyed by the scheme Kubernetes reports in nodeInfo.containerRuntimeVersion,
  valued by the Helm values chaos-daemon needs for it."
  {"containerd" {:chaosDaemon.runtime "containerd"
                 :chaosDaemon.socketPath "/run/containerd/containerd.sock"}
   "cri-o"      {:chaosDaemon.runtime "crio"
                 :chaosDaemon.socketPath "/var/run/crio/crio.sock"}
   "docker"     {:chaosDaemon.runtime "docker"
                 :chaosDaemon.socketPath "/var/run/docker.sock"}})

(defn node-runtimes
  "The distinct container runtimes of the cluster's nodes, e.g. #{\"containerd\"}."
  [test]
  (->> (k8s/nodes test)
       :items
       (keep #(get-in % [:status :nodeInfo :containerRuntimeVersion]))
       (map #(first (str/split % #"://")))
       set))

(defn detect-runtime-values
  "Helm values pinning chaos-daemon to the cluster's container runtime.

  The chart defaults to Docker, which no cluster on Kubernetes 1.24 or later
  runs, so leaving it alone makes network, clock and file I/O faults fail to
  inject while still looking applied. Returns nil when the runtime can't be
  determined, whether because the cluster can't be classified or because the
  lookup itself failed; setup! turns that into an error unless the caller
  pinned the runtime."
  [test]
  (try
    (let [runtimes (node-runtimes test)]
      (if-let [values (and (= 1 (count runtimes))
                           (runtime-values (first runtimes)))]
        (do (info "chaos-daemon container runtime:"
                  (:chaosDaemon.runtime values))
            values)
        (do (warn "Can't tell the container runtime of the cluster; leaving"
                  "chaos-daemon on the chart default. Faults may not inject."
                  {:runtimes runtimes})
            nil)))
    (catch Exception e
      (warn e "Failed to look up the container runtime of the cluster")
      nil)))

(def runtime-key :chaosDaemon.runtime)
(def socket-path-key :chaosDaemon.socketPath)

(defn- nest-keys
  "Turns Helm --set style dotted keys into the nested map a values file wants:
  {:chaosDaemon.runtime \"containerd\"} -> {:chaosDaemon {:runtime \"containerd\"}}"
  [flat]
  (reduce (fn [acc [k v]]
            (assoc-in acc (mapv keyword (str/split (name k) #"\.")) v))
          {}
          flat))

(defn- write-values-file!
  "Renders Helm values to a temp file and returns its path."
  [values]
  (let [file (java.io.File/createTempFile "chaos-mesh-detected-" ".yaml")]
    (.deleteOnExit file)
    (spit file (yaml/generate-string (nest-keys values)))
    (.getPath file)))

(defn- values-file-pins
  "The chaos-daemon keys a caller's Helm values file sets, if it can be read.

  An unreadable file isn't this function's problem; Helm reports it itself."
  [path]
  (try
    (let [daemon (:chaosDaemon (yaml/parse-string (slurp path)))]
      (cond-> #{}
        (contains? daemon :runtime)    (conj runtime-key)
        (contains? daemon :socketPath) (conj socket-path-key)))
    (catch Exception e
      (warn "Couldn't read a Helm values file to check for chaos-daemon"
            "overrides; assuming it sets none"
            {:path path :error (.getMessage e)})
      #{})))

(defn- chaos-daemon-pins
  "The chaos-daemon keys the caller pinned themselves, across :values and :set.

  Both channels count: --set beats every -f, and a later -f beats the detected
  values file, so either one can decide the runtime."
  [values set]
  (into (reduce into #{} (map values-file-pins values))
        (filter #{runtime-key socket-path-key} (keys set))))

(defn- check-pins!
  "Throws when a caller pins a runtime without the socket path that goes with it.

  The two are one setting spread over two keys. Overriding the runtime alone
  leaves the detected socket path in place, and chaos-daemon then talks to the
  wrong socket: every fault but pod chaos looks applied and injects nothing.
  Dropping the detected socket path instead would be the same trap, since the
  chart's default is Docker's."
  [pins]
  (when (and (contains? pins runtime-key)
             (not (contains? pins socket-path-key)))
    (throw (ex-info (str "pinning " (name runtime-key) " also requires "
                         (name socket-path-key)
                         "; the detected socket path belongs to the detected"
                         " runtime, so leaving it in place would point"
                         " chaos-daemon at the wrong socket")
                    {:pinned pins :missing socket-path-key}))))

(defn- check-detected!
  "Throws when the runtime could be neither detected nor supplied.

  Detection returns nil both for a cluster it can't classify and for a lookup
  that failed outright, and either way chaos-daemon falls back to the chart's
  Docker default, which no cluster on Kubernetes 1.24 or later runs. A caller
  who pinned the runtime doesn't need detection and isn't affected."
  [detected pins]
  (when-not (or detected (contains? pins runtime-key))
    (throw (ex-info (str "couldn't determine the chaos-daemon container runtime"
                         " and no " (name runtime-key) " was supplied; the"
                         " chart would fall back to Docker and faults would"
                         " stop injecting. Pass :set or :values pinning "
                         (name runtime-key) " and " (name socket-path-key))
                    {:required [runtime-key socket-path-key]}))))

(defn setup!
  "Installs Chaos Mesh with Helm.

  Options:
    :namespace  Kubernetes namespace. Defaults to chaos-mesh.
    :release    Helm release. Defaults to chaos-mesh.
    :repo-name  Helm repo name. Defaults to chaos-mesh.
    :repo-url   Helm repo URL. Defaults to https://charts.chaos-mesh.org.
    :chart      Helm chart. Defaults to chaos-mesh/chaos-mesh.
    :version    Optional chart version.
    :values     Optional Helm values files.
    :set        Optional Helm --set map.

  The chaos-daemon container runtime detected from the cluster is passed as the
  first Helm values file, so :values and :set both override it the way Helm
  normally resolves them: a later -f beats an earlier one, and --set beats
  every -f. Pinning :chaosDaemon.runtime requires :chaosDaemon.socketPath
  beside it, and a runtime that can be neither detected nor pinned throws."
  [test {:keys [namespace release repo-name repo-url chart version values set]
         :or {namespace default-namespace
              release default-release
              repo-name default-repo-name
              repo-url default-repo-url
              chart default-chart
              version default-version}}]
  (helm/repo-add! test repo-name repo-url)
  ;; Read what the caller asked for before detection runs: a caller who pinned
  ;; the runtime doesn't need detection, so a cluster whose nodes can't be
  ;; listed shouldn't fail their run.
  (let [pins     (chaos-daemon-pins values set)
        _        (check-pins! pins)
        detected (when-not (contains? pins runtime-key)
                   (detect-runtime-values test))
        _        (check-detected! detected pins)
        values   (cond->> values
                   detected (cons (write-values-file! detected)))]
    (apply helm/helm! test
           (concat [:install release chart
                    :-n namespace :--create-namespace
                    :--version version]
                   (mapcat #(vector :-f %) values)
                   ;; Sort by key so the command is byte-identical run to run.
                   ;; Small maps happen to iterate in insertion order, but a
                   ;; caller passing enough :set keys to spill to a hash map
                   ;; would otherwise reshuffle the logged command.
                   (mapcat (fn [[k v]] [:--set (str (name k) "=" v)])
                           (sort-by key set))))))

(defn wipe!
  "Uninstalls Chaos Mesh with Helm.

  Options:
    :namespace  Kubernetes namespace. Defaults to chaos-mesh.
    :release    Helm release. Defaults to chaos-mesh.
    :timeout    Helm timeout. Defaults to 300s."
  [test {:keys [namespace release timeout]
         :or {namespace default-namespace
              release default-release
              timeout default-wipe-timeout}}]
  (helm/helm! test :uninstall release
              :-n namespace
              :--timeout timeout
              :--ignore-not-found))

(def default-fault-opts
  "Per-fault defaults. Options passed to nemesis-package are merged over these,
  one level deep, so a caller overriding a single key of a fault keeps the rest
  of that fault's defaults."
  {:partition {:targets [:one :majority :majorities-ring :minority-third]}
   :packet    {:targets [:one :minority :majority :minority-third :all]
               :behaviors (mapv (fn [[k v]] {k v}) net/all-packet-behaviors)}
   :kill      {:targets [:one]}
   :pause     {:targets [:one]}})

(defn nemesis-package
  "Nemeses with Chaos Mesh backends.

  Supported faults: :partition, :packet, :kill, :pause, :clock, :file-io.

  The optional fourth argument configures individual fault packages. Each fault
  key takes a nested map, merged one level deep over the defaults in
  default-fault-opts, so overriding one key of a fault keeps the rest:

    {:kill    {:targets [:all]}
     :file-io {:volume-path \"/mounted/volume\"
               :file-path   \"/mounted/volume/path/**/*\"}}

  :volume-path and :file-path are required for a :file-io fault; see
  jepsen.k8s.chaos-mesh.file-io/file-io-package for the rest of its options."
  ([db interval faults]
   (nemesis-package db interval faults {}))
  ([db interval faults options]
   (let [opts (-> (merge-with merge default-fault-opts options)
                  (assoc :db db
                         :interval interval
                         :faults (set faults)
                         :dir (or (:dir options)
                                  (System/getProperty "java.io.tmpdir"))))]
     (jn/compose-packages [(network/partition-package opts)
                           (network/packet-package opts)
                           (clock/clock-package opts)
                           (pod/pod-package opts)
                           (file-io/file-io-package opts)]))))
