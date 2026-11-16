(ns jepsen.k8s.chaos-mesh.stress-probe-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.probe :as probe]
            [jepsen.k8s.chaos-mesh.stress-probe :as stress-probe]
            [jepsen.k8s.core :as k8s]))

(def ^:private parse-cgroup-output #'stress-probe/parse-cgroup-output)

(def cgroup-v2-output
  (str "[cgroup.version]\n"
       "2\n"
       "[cpu.stat]\n"
       "usage_usec 1234\n"
       "nr_throttled 5\n"
       "[cpu.pressure]\n"
       "some avg10=1.25 avg60=0.50 avg300=0.10 total=99\n"
       "[memory.current]\n"
       "268435456\n"
       "[memory.max]\n"
       "max\n"
       "[memory.events]\n"
       "oom 1\n"
       "oom_kill 0\n"
       "[memory.pressure]\n"
       "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"))

(deftest parse-cgroup-v2-output-test
  (is (= {:version 2
          :cpu {:stat {:usage_usec 1234
                       :nr_throttled 5}
                :pressure {:some {:avg10 1.25
                                  :avg60 0.5
                                  :avg300 0.1
                                  :total 99}}}
          :memory {:current 268435456
                   :max :max
                   :events {:oom 1 :oom_kill 0}
                   :pressure {:full {:avg10 0.0
                                     :avg60 0.0
                                     :avg300 0.0
                                     :total 0}}}}
         (parse-cgroup-output cgroup-v2-output))))

(deftest parse-cgroup-v1-output-test
  (is (= {:version 1
          :cpu {:usage-ns 999
                :stat {:nr_periods 10 :nr_throttled 2}}
          :memory {:current 1024 :max 2048 :failcnt 3}}
         (parse-cgroup-output
          (str "[cgroup.version]\n1\n"
               "[cpu.usage-ns]\n999\n"
               "[cpu.stat]\nnr_periods 10\nnr_throttled 2\n"
               "[memory.current]\n1024\n"
               "[memory.max]\n2048\n"
               "[memory.failcnt]\n3\n")))))

(deftest stress-probe-samples-configured-containers-test
  (let [calls (atom [])
        p     (stress-probe/probe)
        test  {:k8s {:namespace "database"}}]
    (with-redefs [k8s/kubectl-json!
                  (fn [_test & args]
                    (swap! calls conj [:get args])
                    {:metadata {:uid "pod-uid-1"}
                     :spec {:containers [{:name "postgres"}
                                         {:name "sidecar"}]}})
                  k8s/kubectl!
                  (fn [_test & args]
                    (swap! calls conj [:exec args])
                    cgroup-v2-output)]
      (let [result (probe/sample! p test {:targets ["postgres-0"]
                                          :container-names ["postgres"
                                                            "sidecar"]})]
        (is (= ["postgres" "sidecar"]
               (mapv :container (:containers result))))
        (is (every? #(= "pod-uid-1" (:pod-uid %))
                    (:containers result)))
        (is (every? #(= :sampled (:status %)) (:containers result)))
        (is (= 2 (count (filter #(= :exec (first %)) @calls))))))))

(deftest stress-probe-defaults-to-first-container-test
  (let [p (stress-probe/probe)]
    (with-redefs [k8s/kubectl-json!
                  (fn [& _]
                    {:metadata {:uid "pod-uid-1"}
                     :spec {:containers [{:name "postgres"}
                                         {:name "sidecar"}]}})
                  k8s/kubectl! (fn [& _] cgroup-v2-output)]
      (is (= ["postgres"]
             (->> (probe/sample! p
                                 {:k8s {:namespace "database"}}
                                 {:targets ["postgres-0"]})
                  :containers
                  (mapv :container)))))))

(deftest stress-probe-records-container-errors-test
  (let [p (stress-probe/probe)]
    (with-redefs [k8s/kubectl-json!
                  (fn [& _]
                    {:metadata {:uid "pod-uid-1"}
                     :spec {:containers [{:name "postgres"}]}})
                  k8s/kubectl! (fn [& _] (throw (ex-info "exec failed" {})))]
      (let [sample (-> (probe/sample! p
                                      {:k8s {:namespace "database"}}
                                      {:targets ["postgres-0"]})
                       :containers
                       first)]
        (is (= :error (:status sample)))
        (is (= "exec failed" (:error sample)))))))
