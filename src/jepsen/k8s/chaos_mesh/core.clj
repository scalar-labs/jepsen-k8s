(ns jepsen.k8s.chaos-mesh.core
  "Composing Jepsen nemeses with k8s/Chaos Mesh backends."
  (:require [clojure.string :as str]
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
  determined; pass :set explicitly for such a cluster."
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
    :set        Optional Helm --set map. Takes precedence over the
                chaos-daemon container runtime detected from the cluster."
  [test {:keys [namespace release repo-name repo-url chart version values set]
         :or {namespace default-namespace
              release default-release
              repo-name default-repo-name
              repo-url default-repo-url
              chart default-chart
              version default-version}}]
  (helm/repo-add! test repo-name repo-url)
  (let [set (merge (detect-runtime-values test) set)]
    (apply helm/helm! test
           (concat [:install release chart
                    :-n namespace :--create-namespace
                    :--version version]
                   (mapcat #(vector :-f %) values)
                   (mapcat (fn [[k v]] [:--set (str (name k) "=" v)]) set)))))

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

(defn nemesis-package
  "Nemeses with Chaos Mesh backends.

  The optional fourth argument configures individual fault packages. For a
  :file-io fault it must contain:

    {:file-io {:volume-path \"/mounted/volume\"
               :file-path   \"/mounted/volume/path/**/*\"}}"
  ([db interval faults]
   (nemesis-package db interval faults {}))
  ([db interval faults options]
   (let [opts (-> (or options {})
                  (assoc :db db
                         :interval interval
                         :faults (set faults)
                         :dir (or (:dir options)
                                  (System/getProperty "java.io.tmpdir")))
                  (update :partition
                          #(merge {:targets [:one :majority :majorities-ring
                                             :minority-third]}
                                  %))
                  (update :packet
                          #(merge {:targets [:one :minority :majority
                                             :minority-third :all]
                                   :behaviors
                                   (reduce (fn [acc [k v]] (conj acc {k v}))
                                           []
                                           net/all-packet-behaviors)}
                                  %))
                  (update :kill #(merge {:targets [:one]} %))
                  (update :pause #(merge {:targets [:one]} %)))]
     (jn/compose-packages [(network/partition-package opts)
                           (network/packet-package opts)
                           (clock/clock-package opts)
                           (pod/pod-package opts)
                           (file-io/file-io-package opts)]))))
