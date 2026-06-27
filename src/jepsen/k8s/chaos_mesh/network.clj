(ns jepsen.k8s.chaos-mesh.network
  "NetworkChaos helpers.
  Supported faults: :partition, :delay, :loss, :corrupt, :duplicate, :reorder, :rate."
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.net :as net]
            [jepsen.nemesis.combined :as jn]
            [jepsen.util :as util]))

(def ^:private partition-label
  "Label applied to every partition NetworkChaos so they can be torn down
  together, regardless of how many rules a grudge produced."
  :jepsen-partition)

(defn- pod-grudge
  "Computes a grudge over the given pods for a partition spec. Mirrors
  jepsen.nemesis.combined/grudge, but operates on the live Kubernetes pods
  rather than the test's placeholder :nodes."
  [pods part-spec]
  (case part-spec
    :one              (n/complete-grudge (n/split-one pods))
    :majority         (n/complete-grudge (n/bisect (shuffle pods)))
    :majorities-ring  (n/majorities-ring pods)
    :minority-third   (n/complete-grudge (split-at (util/minority-third
                                                    (count pods))
                                                   (shuffle pods)))
    part-spec))

(defn- grudge->rules
  "Turns a grudge ({node #{nodes it drops inbound traffic from}}) into partition
  rules. Mirrors jepsen.net/drop-all!: each node drops inbound traffic from the
  pods in its grudge set. Nodes sharing the same drop-set are grouped into one
  rule to keep the resource count down; the result faithfully reproduces any
  grudge, including asymmetric ones such as :majorities-ring."
  [grudge]
  (->> grudge
       (filter (fn [[_node sources]] (seq sources)))
       (group-by (fn [[_node sources]] sources))
       (map (fn [[sources entries]]
              {:nodes   (mapv first entries)
               :sources (vec sources)}))))

(defn- make-partition-manifest
  "A NetworkChaos that blocks traffic from `sources` to `nodes` (direction
  :from), the Chaos Mesh equivalent of dropping inbound traffic on `nodes`."
  [test name nodes sources]
  (let [ns (k8s/namespace test)]
    (yaml/generate-string
     {:apiVersion "chaos-mesh.org/v1alpha1"
      :kind "NetworkChaos"
      :metadata {:name name
                 :namespace "chaos-mesh"
                 :labels {partition-label "true"}}
      :spec {:action "partition"
             :mode "all"
             :selector {:pods {ns (vec nodes)}}
             :direction "from"
             :target {:mode "all"
                      :selector {:pods {ns (vec sources)}}}}})))

(defn- partition-chaos-names
  "Names of the partition NetworkChaos resources currently applied."
  [test]
  (try
    (->> (k8s/kubectl-lines! test :get :networkchaos
                             :-n "chaos-mesh"
                             :-l (str (name partition-label) "=true")
                             :-o :name
                             :--request-timeout "10s")
         (mapv (fn [line] (last (str/split line #"/")))))
    (catch Exception _ [])))

(defn- apply-partition!
  [test grudge dir]
  (doseq [[i {:keys [nodes sources]}] (map-indexed vector (grudge->rules grudge))]
    (exp/apply! test
                (make-partition-manifest test (str "partition-" i) nodes sources)
                dir))
  {:isolated grudge})

(defn- stop-partition!
  [test]
  (doseq [name (partition-chaos-names test)]
    (exp/stop! test {:name name :kind "networkchaos"}))
  :network-healed)

(defn- partition-nemesis
  "Partition nemesis for Chaos Mesh. Computes the grudge over the live pods in
  the test namespace, so partitions target real Kubernetes pods rather than
  Jepsen's placeholder :nodes."
  [opts]
  (reify
    n/Reflection
    (fs [_this] [:start-partition :stop-partition])

    n/Nemesis
    (setup! [this test]
      (stop-partition! test)
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result (case f
                     :start-partition
                     (let [pods   (k8s/pod-names test {})
                           grudge (pod-grudge pods value)]
                       (apply-partition! test grudge (:dir opts)))
                     :stop-partition (stop-partition! test))]
        (assoc op :value result)))

    (teardown! [_this test]
      (stop-partition! test))))

(defn partition-package
  "Replace partition-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/partition-package opts)
         :nemesis (partition-nemesis opts)))

(defn- pod-targets
  "Selects pods from the test namespace for the given node spec. Operates on the
  live Kubernetes pods rather than the test's placeholder :nodes."
  [pods node-spec]
  (case node-spec
    nil             (util/random-nonempty-subset pods)
    :one            (list (rand-nth pods))
    :minority       (take (dec (util/majority (count pods))) (shuffle pods))
    :majority       (take (util/majority (count pods)) (shuffle pods))
    :minority-third (take (util/minority-third (count pods)) (shuffle pods))
    :all            pods
    node-spec))

(defn- make-packet-manifest
  [test targets behaviour]
  (let [[kind params] (first behaviour)
        action (case kind
                 :reorder :delay
                 :rate :netem
                 kind)
        get-percent-fn #(-> % :percent name (str/replace "%" ""))
        get-correlation-fn #(-> % :correlation name (str/replace "%" ""))
        base {:apiVersion "chaos-mesh.org/v1alpha1"
              :kind "NetworkChaos"
              :metadata {:name kind :namespace "chaos-mesh"}
              :spec {:action action
                     :mode "all"
                     :selector {:pods {(k8s/namespace test) (vec targets)}}}}
        fault-spec (case kind
                     :delay {:latency (-> params :time name)
                             :jitter (-> params :jitter name)
                             :correlation (get-correlation-fn params)}
                     :loss {:loss (get-percent-fn params)
                            :correlation (get-correlation-fn params)}
                     :corrupt {:corrupt (get-percent-fn params)
                               :correlation (get-correlation-fn params)}
                     :duplicate {:duplicate (get-percent-fn params)
                                 :correlation (get-correlation-fn params)}
                     ;; with delay fault
                     :reorder {:latency (-> net/all-packet-behaviors :delay :time name)
                               :jitter (-> net/all-packet-behaviors :delay :jitter name)
                               :correlation (-> net/all-packet-behaviors
                                                :delay
                                                get-correlation-fn)
                               :reorder {:reorder (get-percent-fn params)
                                         :correlation (get-correlation-fn params)}}
                     :rate {:rate (-> params :rate name (str/replace "bit" "bps"))})]
    (->> (assoc-in base [:spec (if (= kind :rate) :rate action)] fault-spec)
         yaml/generate-string)))

(defn- apply-packet!
  [test targets behaviour dir]
  (let [manifest (make-packet-manifest test targets behaviour)]
    (exp/apply! test manifest dir)
    {:fault-kind (-> behaviour first first)
     :targets targets}))

(defn- stop-packet!
  [test]
  (mapv #(exp/stop! test {:name % :kind "networkchaos"})
        ["delay" "loss" "corrupt" "duplicate" "reorder" "rate"])
  :packet-healed)

(defn- packet-nemesis
  "A nemesis to disrupt packets with Chaos Mesh."
  [opts]
  (reify
    n/Reflection
    (fs [_this]
      [:start-packet  :stop-packet])

    n/Nemesis
    (setup! [this test]
      (stop-packet! test)
      this)

    (invoke! [_ test {:keys [f value] :as op}]
      (let [result (case f
                     :start-packet (let [[spec behavior] value
                                         pods    (k8s/pod-names test {})
                                         targets (pod-targets pods spec)]
                                     (apply-packet! test
                                                    targets
                                                    behavior
                                                    (:dir opts)))
                     :stop-packet  (stop-packet! test))]
        (assoc op :value result)))

    (teardown! [_ test]
      (stop-packet! test))))

(defn packet-package
  "Replace packet-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/packet-package opts)
         :nemesis (packet-nemesis opts)))
