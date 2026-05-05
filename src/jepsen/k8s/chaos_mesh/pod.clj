(ns jepsen.k8s.chaos-mesh.pod
  "PodChaos helpers. Supports :pause and :kill faults,
  which translate to 'pod-failure' and 'pod-kill' actions in Chaos Mesh, respectively."
  (:require [clj-yaml.core :as yaml]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as jn]))

(def ^:private ^:const MAX_KILLS 3)

(defn- make-manifest
  [test fault targets]
  (let [action (case fault
                 :pause "pod-failure"
                 :kill "pod-kill"
                 (throw (ex-info "Unexpected pod-fault type" {:fault fault})))
        base-spec {:action action
                   :mode "all"
                   :selector {:pods {(k8s/namespace test) targets}}}
        spec (if (= action "pod-failure")
               (assoc base-spec :duration "60s")
               base-spec)]
    (yaml/generate-string
     {:apiVersion "chaos-mesh.org/v1alpha1"
      :kind "PodChaos"
      :metadata {:name action :namespace "chaos-mesh"}
      :spec spec})))

(defn- apply!
  [test fault dir]
  ;; TODO: choose targets from the specified pods
  (let [targets (exp/choose-targets test MAX_KILLS)
        manifest (make-manifest test fault targets)]
    (exp/apply! test manifest dir)
    {fault targets}))

(defn- stop!
  [test]
  (mapv #(exp/stop! test {:name % :kind "podchaos"})
        ["pod-failure" "pod-kill"])
  :pod-healed)

(defn- pod-nemesis
  "Based on the db-nemesis in jepsen.control,
   but with Chaos Mesh operations instead of process control."
  [opts]
  (reify
    n/Reflection
    (fs [_] #{:start :kill :pause :resume})

    n/Nemesis
    (setup! [this test]
      (stop! test)
      this)

    (invoke! [_ test op]
      (let [dir (:dir opts)
            result (case (:f op)
                     :start  (stop! test)
                     :kill   (apply! test :kill dir)
                     :pause  (apply! test :pause dir)
                     :resume (stop! test))]
        (assoc op :value result)))

    (teardown! [_ test]
      (stop! test))))

(defn pod-package
  "Replace db-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/db-package opts)
         :nemesis (pod-nemesis opts)))
