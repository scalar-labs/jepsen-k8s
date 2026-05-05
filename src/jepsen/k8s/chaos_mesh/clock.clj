(ns jepsen.k8s.chaos-mesh.clock
  "Nemesis for clock faults using Chaos Mesh's TimeChaos."
  (:require [clj-yaml.core :as yaml]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.time :as nt]))

(def ^:private ^:const MAX_CLOCK_PODS 3)

(defn- make-bump-manifest
  [target delta-ms]
  (let [time-offset (if (>= (abs delta-ms) 1000)
                      (str (quot delta-ms 1000) "s"
                           (mod (abs delta-ms) 1000) "ms")
                      (str delta-ms "ms"))]
    (->> (yaml/generate-string
          {:apiVersion "chaos-mesh.org/v1alpha1"
           :kind "TimeChaos"
           :metadata {:name (str "time-bump-" target)
                      :namespace "chaos-mesh"}
           :spec {:mode "all"
                  :selector {:pods {(k8s/namespace test) [target]}}
                  :timeOffset time-offset}}))))

(defn- apply-bump!
  [test target delta-ms dir]
  (let [manifest (make-bump-manifest target delta-ms)]
    (exp/apply! test manifest dir)
    {:target target :delta-ms delta-ms}))

(defn- stop-bump!
  [test target]
  (exp/stop! test {:name (str "time-bump-" target) :kind "timechaos"})
  {:target target})

(defn- clock-nemesis
  []
  (reify n/Nemesis
    (setup! [this test]
      (mapv #(stop-bump! test %) (k8s/pod-names test {}))
      this)

    (invoke! [_ test op]
      (c/on (-> test :nodes first)
            (let [dir (:dir op)
                  res (case (:f op)
                        :reset (mapv #(stop-bump! test %) (:value op))
                        :bump (mapv (fn [[target delta-ms]]
                                      (stop-bump! test target)
                                      (apply-bump! test target delta-ms dir))
                                    (:value op)))]
              (assoc op :clock-offsets res))))

    (teardown! [_ test]
      (mapv #(stop-bump! test %) (k8s/pod-names test {})))))

(defn clock-package
  "Copied from nemesis.combine/clock-package. Modified for Chaos Mesh."
  [opts]
  ;; TODO: support check-offsets and strobe
  (let [needed? ((:faults opts) :clock)
        nemesis (n/compose {{:reset-clock :reset
                             :bump-clock  :bump}
                            (clock-nemesis)})
        target-select (fn [test]
                        ;; TODO: choose targets from the specified pods
                        (exp/choose-targets test MAX_CLOCK_PODS))
        clock-gen (gen/mix [(nt/reset-gen-select target-select)
                            (nt/bump-gen-select  target-select)])
        gen (->> clock-gen
                 (gen/f-map {:reset :reset-clock
                             :bump  :bump-clock})
                 (gen/stagger (:interval opts)))]
    {:generator         (when needed? gen)
     :final-generator   (when needed? {:type :info, :f :reset-clock})
     :nemesis           nemesis
     :perf              #{{:name  "clock"
                           :start #{:bump-clock}
                           :stop  #{:reset-clock}
                           :color "#A0E9E3"}}}))
