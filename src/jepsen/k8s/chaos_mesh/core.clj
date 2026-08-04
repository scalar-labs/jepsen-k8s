(ns jepsen.k8s.chaos-mesh.core
  "Composing Jepsen nemeses with k8s/Chaos Mesh backends."
  (:require [jepsen.k8s.chaos-mesh
             [clock :as clock]
             [file-io :as file-io]
             [network :as network]
             [pod :as pod]]
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
    :set        Optional Helm --set map."
  [test {:keys [namespace release repo-name repo-url chart version values set]
         :or {namespace default-namespace
              release default-release
              repo-name default-repo-name
              repo-url default-repo-url
              chart default-chart
              version default-version}}]
  (helm/repo-add! test repo-name repo-url)
  (apply helm/helm! test
         (concat [:install release chart
                  :-n namespace :--create-namespace
                  :--version version]
                 (mapcat #(vector :-f %) values)
                 (mapcat (fn [[k v]] [:--set (str (name k) "=" v)]) set))))

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
