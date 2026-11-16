(ns jepsen.k8s.chaos-mesh.stress-test
  (:require [clj-yaml.core :as yaml]
            [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.chaos-mesh.probe :as probe]
            [jepsen.k8s.chaos-mesh.stress :as stress]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]))

(def ^:private validate-config #'stress/validate-config)
(def ^:private make-manifest #'stress/make-manifest)
(def ^:private stress-nemesis #'stress/stress-nemesis)

(def config
  {:pod-selector {:app "postgres"}
   :container-names ["postgres"]
   :targets [:one]
   :cpu {:workers 2 :load 80}
   :memory {:workers 3
            :size "256MB"
            :oom-score-adj -500}})

(deftest config-validation-test
  (testing "CPU defaults to one fully loaded worker"
    (is (= {:targets [:one]
            :cpu {:workers 1 :load 100}}
           (validate-config {:cpu {}}))))
  (testing "memory defaults to one worker"
    (is (= {:targets [:one]
            :memory {:workers 1 :size "25%"}}
           (validate-config {:memory {:size "25%"}}))))
  (testing "CPU and memory can be enabled together"
    (is (= #{:cpu :memory}
           (-> (validate-config config)
               (select-keys [:cpu :memory])
               keys
               set))))
  (testing "at least one stressor is required"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"requires :cpu, :memory, or both"
                          (validate-config {}))))
  (testing "CPU workers and load are bounded"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"cpu workers"
                          (validate-config {:cpu {:workers 0}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"cpu load"
                          (validate-config {:cpu {:load 101}}))))
  (testing "memory size is required"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"memory size"
                          (validate-config {:memory {}}))))
  (testing "memory OOM score is validated"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"oom-score-adj"
                          (validate-config {:memory {:size "1GB"
                                                     :oom-score-adj 1001}}))))
  (testing "the optional probe configuration is validated"
    (is (:probe? (validate-config {:cpu {} :probe? true})))
    (is (= 250
           (:probe-interval-ms
            (validate-config {:cpu {}
                              :probe? true
                              :probe-interval-ms 250}))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"probe\? must be a boolean"
                          (validate-config {:cpu {} :probe? :yes})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"probe-interval-ms"
                          (validate-config {:cpu {}
                                            :probe-interval-ms 0})))))

(deftest target-validation-test
  (testing "symbolic and nested explicit targets are accepted"
    (doseq [targets [[:one] [:all] [nil]
                     [:one :minority :majority :minority-third :all]
                     [["pod-0" "pod-1"]]
                     [:one ["pod-0"]]]]
      (is (= targets
             (:targets (validate-config {:cpu {} :targets targets}))))))
  (testing "a bare list of pod names is rejected before a run starts"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"has to be nested"
                          (validate-config {:cpu {}
                                            :targets ["pod-0" "pod-1"]})))))

(deftest combined-manifest-test
  (let [manifest (-> (make-manifest
                      {:k8s {:namespace "database"}}
                      ["postgres-1"]
                      (validate-config config))
                     yaml/parse-string)]
    (is (= "StressChaos" (:kind manifest)))
    (is (= {:name "stress-fault" :namespace "chaos-mesh"}
           (:metadata manifest)))
    (is (= "all" (get-in manifest [:spec :mode])))
    (is (= ["postgres-1"]
           (get-in manifest [:spec :selector :pods :database])))
    (is (= ["postgres"] (get-in manifest [:spec :containerNames])))
    (is (= {:workers 2 :load 80}
           (get-in manifest [:spec :stressors :cpu])))
    (is (= {:workers 3
            :size "256MB"
            :oomScoreAdj -500}
           (get-in manifest [:spec :stressors :memory])))))

(deftest individual-stressor-manifest-test
  (testing "CPU can be applied without memory"
    (let [manifest (-> (make-manifest
                        {:k8s {:namespace "database"}}
                        ["pod-0"]
                        (validate-config {:cpu {}}))
                       yaml/parse-string)]
      (is (= {:cpu {:workers 1 :load 100}}
             (get-in manifest [:spec :stressors])))))
  (testing "memory can be applied without CPU"
    (let [manifest (-> (make-manifest
                        {:k8s {:namespace "database"}}
                        ["pod-0"]
                        (validate-config {:memory {:size "25%"}}))
                       yaml/parse-string)]
      (is (= {:memory {:workers 1 :size "25%"}}
             (get-in manifest [:spec :stressors]))))))

(deftest nemesis-selects-from-eligible-pods-test
  (let [pod-query (atom nil)
        applied   (atom nil)
        stops     (atom 0)
        nemesis   (stress-nemesis (validate-config config) "/tmp")]
    (with-redefs [k8s/pod-names
                  (fn [_test opts]
                    (reset! pod-query opts)
                    ["postgres-0" "postgres-1"])
                  exp/stop!
                  (fn [& _]
                    (swap! stops inc))
                  exp/apply!
                  (fn [_test manifest dir]
                    (reset! applied {:manifest (yaml/parse-string manifest)
                                     :dir dir}))]
      (let [result (n/invoke! nemesis
                              {:k8s {:namespace "database"}}
                              {:type :info
                               :f :start-stress
                               :value ["postgres-1" "not-eligible"]})]
        (is (= {:selector {:app "postgres"}} @pod-query))
        (is (= ["postgres-1"] (get-in result [:value :targets])))
        (is (= (select-keys (validate-config config) [:cpu :memory])
               (get-in result [:value :stressors])))
        (is (= ["postgres-1"]
               (get-in @applied
                       [:manifest :spec :selector :pods :database])))
        (is (= "/tmp" (:dir @applied)))
        (is (= 1 @stops))))))

(deftest nemesis-defaults-to-all-pods-test
  (let [pod-query (atom ::unset)
        config    (validate-config (dissoc config :pod-selector))
        nemesis   (stress-nemesis config "/tmp")]
    (is (not (contains? config :pod-selector)))
    (with-redefs [k8s/pod-names
                  (fn [_test opts]
                    (reset! pod-query opts)
                    ["postgres-0"])
                  exp/stop!  (fn [& _])
                  exp/apply! (fn [& _])]
      (let [result (n/invoke! nemesis
                              {:k8s {:namespace "database"}}
                              {:type :info :f :start-stress :value :one})]
        (is (= {:selector nil} @pod-query))
        (is (= ["postgres-0"] (get-in result [:value :targets])))))))

(deftest nemesis-rejects-empty-selection-test
  (let [nemesis (stress-nemesis (validate-config config) "/tmp")]
    (with-redefs [k8s/pod-names (fn [& _] [])]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"selected no eligible pods"
                            (n/invoke! nemesis
                                       {:k8s {:namespace "database"}}
                                       {:type :info
                                        :f :start-stress
                                        :value :one}))))))

(deftest failed-apply-rolls-back-test
  (let [stops   (atom 0)
        nemesis (stress-nemesis (validate-config config) "/tmp")]
    (with-redefs [k8s/pod-names (fn [& _] ["postgres-0"])
                  exp/stop! (fn [& _] (swap! stops inc))
                  exp/apply! (fn [& _] (throw (ex-info "apply failed" {})))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"apply failed"
                            (n/invoke! nemesis
                                       {:k8s {:namespace "database"}}
                                       {:type :info
                                        :f :start-stress
                                        :value :one})))
      (is (= 2 @stops)))))

(deftest lifecycle-test
  (testing "setup and teardown clear configured stress faults"
    (let [calls   (atom [])
          nemesis (stress-nemesis (validate-config config) "/tmp")]
      (with-redefs [exp/stop! (fn [& _] (swap! calls conj :stop!))]
        (n/setup! nemesis {:k8s {:namespace "database"}})
        (n/teardown! nemesis {:k8s {:namespace "database"}}))
      (is (= [:stop! :stop!] @calls))))
  (testing "a run that never asked for the fault still sweeps a leftover one"
    (let [calls   (atom [])
          package (stress/stress-package {:faults #{:kill} :dir "/tmp"})]
      (with-redefs [exp/stop! (fn [& _] (swap! calls conj :stop!))]
        (n/setup! (:nemesis package) {:k8s {:namespace "database"}})
        (n/teardown! (:nemesis package) {:k8s {:namespace "database"}}))
      (is (= [:stop! :stop!] @calls)))))

(deftest probe-lifecycle-test
  (let [calls (atom [])]
    (with-redefs [probe/runner
                  (fn [_probe opts]
                    (swap! calls conj [:runner opts])
                    ::runner)
                  probe/start!
                  (fn [_runner _test context]
                    (swap! calls conj [:probe-start context]))
                  probe/stop!
                  (fn [_runner]
                    (swap! calls conj :probe-stop))
                  k8s/pod-names (fn [& _] ["postgres-0"])
                  exp/stop! (fn [& _] (swap! calls conj :fault-stop))
                  exp/apply! (fn [& _] (swap! calls conj :fault-apply))]
      (let [config  (validate-config (assoc config
                                            :probe? true
                                            :probe-interval-ms 250))
            nemesis (stress-nemesis config "/tmp")
            test    {:k8s {:namespace "database"}}]
        (is (= [[:runner {:interval-ms 250}]] @calls))

        (reset! calls [])
        (n/invoke! nemesis test
                   {:type :info :f :start-stress :value :one})
        (is (= [:probe-stop
                :fault-stop
                :fault-apply]
               (take 3 @calls)))
        (let [[event context] (last @calls)]
          (is (= :probe-start event))
          (is (= {:name "stress-fault" :kind "stresschaos"}
                 (:experiment context)))
          (is (= "database" (:namespace context)))
          (is (= ["postgres-0"] (:targets context)))
          (is (= ["postgres"] (:container-names context))))

        (reset! calls [])
        (n/invoke! nemesis test
                   {:type :info :f :stop-stress :value nil})
        (is (= [:probe-stop :fault-stop] @calls))

        (reset! calls [])
        (n/setup! nemesis test)
        (is (= [:probe-stop :fault-stop] @calls))

        (reset! calls [])
        (n/teardown! nemesis test)
        (is (= [:probe-stop :fault-stop] @calls))))))

(deftest probe-default-interval-test
  (let [opts (atom nil)]
    (with-redefs [probe/runner
                  (fn [_probe runner-opts]
                    (reset! opts runner-opts)
                    ::runner)]
      (stress-nemesis (validate-config (assoc config :probe? true)) "/tmp")
      (is (= {:interval-ms 5000} @opts)))))

(deftest disabled-probe-does-not-create-runner-test
  (with-redefs [probe/runner
                (fn [& _]
                  (throw (ex-info "runner should not be created" {})))]
    (is (some? (stress-nemesis (validate-config config) "/tmp")))))

(deftest failed-apply-does-not-start-probe-test
  (let [starts (atom 0)]
    (with-redefs [probe/runner (fn [& _] ::runner)
                  probe/stop! (fn [& _])
                  probe/start! (fn [& _] (swap! starts inc))
                  k8s/pod-names (fn [& _] ["postgres-0"])
                  exp/stop! (fn [& _])
                  exp/apply! (fn [& _] (throw (ex-info "apply failed" {})))]
      (let [nemesis (stress-nemesis
                     (validate-config (assoc config :probe? true))
                     "/tmp")]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"apply failed"
                              (n/invoke! nemesis
                                         {:k8s {:namespace "database"}}
                                         {:type :info
                                          :f :start-stress
                                          :value :one})))
        (is (zero? @starts))))))

(deftest package-test
  (let [package (stress/stress-package
                 {:faults #{:stress}
                  :interval 10
                  :dir "/tmp"
                  :stress config})]
    (is (some? (:generator package)))
    (is (= {:type :info :f :stop-stress :value nil}
           (:final-generator package)))
    (is (= #{:start-stress :stop-stress}
           (set (n/fs (:nemesis package)))))))
