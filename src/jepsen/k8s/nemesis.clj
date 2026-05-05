(ns jepsen.k8s.nemesis
  "Cmposing Jepsen nemeses with k8s/Chaos Mesh backends."
  (:require [jepsen.k8s.chaos-mesh.core :as cm]))

(defn chaos-mesh-package
  "Nemeses with Chaos Mesh backends.
  Supported faults: :pause, :kill, :partition, :packet, :clock."
  [db interval faults]
  (cm/nemesis-package db interval faults))
