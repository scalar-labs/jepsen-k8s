(ns jepsen.k8s.chaos-mesh.stress-test
  (:require [clj-yaml.core :as yaml]
            [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
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
            :time "10s"
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
  (testing "memory ramp time and OOM score are validated"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"memory time"
                          (validate-config {:memory {:size "1GB" :time ""}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"oom-score-adj"
                          (validate-config {:memory {:size "1GB"
                                                     :oom-score-adj 1001}})))))

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
            :time "10s"
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
  (testing "an unused package does not touch the cluster"
    (let [calls   (atom [])
          package (stress/stress-package {:faults #{:kill} :dir "/tmp"})]
      (with-redefs [exp/stop! (fn [& _] (swap! calls conj :stop!))]
        (n/setup! (:nemesis package) {:k8s {:namespace "database"}})
        (n/teardown! (:nemesis package) {:k8s {:namespace "database"}}))
      (is (empty? @calls)))))

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
