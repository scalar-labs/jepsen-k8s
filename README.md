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
             ├── NetworkChaos / PodChaos / TimeChaos / IOChaos skeletons
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

Supported faults are `:partition`, `:packet`, `:kill`, `:pause`, `:clock`, and
`:file-io`. An optional fourth argument configures individual fault packages.
Each fault key takes a nested map, merged one level deep over the defaults, so
overriding one key of a fault keeps the rest:

```clojure
(def nemesis
  (chaos-mesh/nemesis-package
    db interval faults
    {:kill    {:targets [:all]}
     :file-io {:volume-path "/var/lib/postgresql/data"
               :file-path   "/var/lib/postgresql/data/pg_wal/**/*"
               :pod-selector {:app "postgres"}}}))
```

`:file-io` requires `:volume-path` and `:file-path`. Its optional keys are
`:pod-selector` (defaults to every pod in the test namespace — this is the
fault's blast radius, so it is worth setting), `:container-names` (all),
`:methods` (`[:read :write]`), `:errno` (5, EIO), `:percent` (100), and
`:targets` (`[:one]`). A `:targets` element that lists pod names has to be
nested, as in `[["pod-0" "pod-1"]]`, because each op uses one element as its
whole spec.

## Notes

- This project assumes an existing kubeconfig/context. It should work with kind, GKE, EKS, AKS, or self-hosted clusters as long as `kubectl` and `helm` can access the target cluster.
- Cloud-specific cluster provisioning is intentionally out of scope for the first version.
- Chaos Mesh availability depends on RBAC, CNI, PodSecurity, and cluster capabilities.
- `chaos-mesh/setup!` pins `chaos-daemon` to the container runtime the cluster's nodes report. The chart defaults to Docker, which no cluster on Kubernetes 1.24 or later runs, and a mismatched runtime leaves every fault but pod chaos unable to inject while still looking applied. The detected values are passed as the first Helm values file, so both `:values` and `:set` override them by Helm's usual precedence: a later `-f` beats an earlier one, and `--set` beats every `-f`. This matters on k3s and RKE2, whose containerd socket is `/run/k3s/containerd/containerd.sock` rather than the detected `/run/containerd/containerd.sock`.
- Pinning `chaosDaemon.runtime` requires `chaosDaemon.socketPath` beside it — the two are one setting in two keys, and a runtime paired with someone else's socket path injects nothing. `setup!` throws rather than install a mismatched pair, and also throws when the runtime can be neither detected nor pinned, rather than fall back to the chart's Docker default.
- The file I/O nemesis needs amd64 nodes. Chaos Mesh injects `IOChaos` with `toda`, whose binary is x86-64 even in the arm64 `chaos-daemon` image, so it dies under emulation and no fault is ever applied. A test that includes the `:file-io` fault checks every node's architecture during nemesis setup and throws before the run starts if any node is not amd64; there is no way to skip the check, because a run whose faults never inject reports no anomalies for the wrong reason.
