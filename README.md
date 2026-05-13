# jepsen-k8s

Kubernetes helpers for Jepsen tests.

Leiningen dependency:

```clojure
[com.scalar-labs/jepsen-k8s "0.1.0-SNAPSHOT"]
```

The goal is to keep Jepsen itself unchanged while providing reusable helpers for systems deployed on Kubernetes:

- `kubectl` / `helm` execution wrappers
- pod / service / node discovery
- pod log and event collection
- wait helpers
- Chaos Mesh based nemesis backends
- run artifact collection

This is intended to be a thin extraction target from existing Jepsen tests such as ScalarDB-on-Kubernetes tests.

## Design

```text
Jepsen test
  ├── db setup / lifecycle
  │     └── jepsen.k8s.*
  │          ├── kubectl / helm wrappers
  │          ├── pod/service/node discovery
  │          ├── wait
  │          └── logs/events/artifact collection
  │
  └── nemesis
        └── jepsen.k8s.chaos-mesh.*
             ├── NetworkChaos / PodChaos / TimeChaos skeletons
             └── apply/delete CRDs
```

## Basic usage

```clojure
(require '[jepsen.k8s.core :as k8s]
         '[jepsen.k8s.helm :as helm]
         '[jepsen.k8s.chaos-mesh.core :as chaos-mesh])

(def test
  {:k8s {:context "kind-jepsen"
         :namespace "jepsen-test"}})

(k8s/kubectl! test :create :ns "jepsen-test")
(helm/install! test {:release "my-db"
                     :chart "./chart"
                     :namespace "jepsen-test"
                     :values ["values.yaml"]})
(k8s/wait! test {:namespace "jepsen-test"
                 :for "condition=Ready"
                 :resource "pod"
                 :selector "app=my-db"
                 :timeout "300s"})

(def nemesis
  (chaos-mesh/nemesis-package db interval faults))
```

## Notes

- This project assumes an existing kubeconfig/context. It should work with kind, GKE, EKS, AKS, or self-hosted clusters as long as `kubectl` and `helm` can access the target cluster.
- Cloud-specific cluster provisioning is intentionally out of scope for the first version.
- Chaos Mesh availability depends on RBAC, CNI, PodSecurity, and cluster capabilities.
