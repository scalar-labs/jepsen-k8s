(ns jepsen.k8s.chaos-mesh.network
  "NetworkChaos helpers.
  Supported faults: :partition, :delay, :loss, :corrupt, :duplicate, :reorder, :rate."
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.net :as net]
            [jepsen.nemesis.combined :as jn]))

(defn- make-partition-manifest
  [test grudge]
  (let [remain (->> (k8s/pod-names test {}) (remove (set grudge)))]
    (->> (yaml/generate-string
          {:apiVersion "chaos-mesh.org/v1alpha1"
           :kind "NetworkChaos"
           :metadata {:name "partition" :namespace "chaos-mesh"}
           :spec {:action "partition"
                  :mode "all"
                  :selector {:pods {"default" remain}}
                  :direction "both"
                  :target {:mode "all"
                           :selector {:pods {(k8s/namespace test) grudge}}}}}))))

(defn- apply-partition!
  [test grudge dir]
  (let [manifest (make-partition-manifest test grudge)]
    (exp/apply! test manifest dir)
    {:isolated grudge}))

(defn- stop-partition!
  [test]
  (exp/stop! test {:name "partition" :kind "networkchaos"})
  :network-healed)

(defn- partitioner
  "Partitioner for Chaos Mesh."
  [opts]
  (reify n/Nemesis
    (setup! [this test]
      (stop-partition! test)
      this)

    (invoke! [_ test op]
      (let [dir (:dir opts)
            grudge (:value op)
            result (case (:f op)
                     :start (apply-partition! test grudge dir)
                     :stop  (stop-partition! test))]
        (assoc op :value result)))

    (teardown! [_ _]
      (stop-partition! test))))

(defn partition-package
  "Replace partition-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/partition-package opts)
         :nemesis (jn/partition-nemesis (:db opts) (partitioner opts))))

(defn- make-packet-manifest
  [targets behaviour]
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
                     :selector {:pods {(k8s/namespace test) targets}}}}
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
  (let [manifest (make-packet-manifest targets behaviour)]
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
                     :start-packet (let [[targets behavior] value]
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
