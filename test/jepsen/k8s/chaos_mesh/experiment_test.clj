(ns jepsen.k8s.chaos-mesh.experiment-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.experiment :as exp]))

(defn- pods
  "n pod names, as k8s/pod-names returns them."
  [n]
  (mapv #(str "pod-" %) (range n)))

(deftest select-targets-size-test
  (testing "how many pods each spec selects, by cluster size"
    ;; Spelled out rather than derived from jepsen.util so a change in either
    ;; the specs or util's arithmetic shows up here. Note the zeroes:
    ;; :minority-third selects nothing below four pods, so a fault using it on
    ;; the usual three-node cluster targets no pod at all.
    (doseq [[size expected]
            {1 {:one 1 :minority 0 :majority 1 :minority-third 0 :all 1}
             2 {:one 1 :minority 1 :majority 2 :minority-third 0 :all 2}
             3 {:one 1 :minority 1 :majority 2 :minority-third 0 :all 3}
             4 {:one 1 :minority 2 :majority 3 :minority-third 1 :all 4}
             5 {:one 1 :minority 2 :majority 3 :minority-third 1 :all 5}}
            [spec n] expected]
      (is (= n (count (exp/select-targets (pods size) spec)))
          (str spec " on " size " pod(s)")))))

(deftest select-targets-picks-real-pods-test
  (let [pods (pods 5)]
    (doseq [spec [nil :one :minority :majority :minority-third :all]
            _trial (range 20)]
      (let [selected (exp/select-targets pods spec)]
        (testing (str spec " selects distinct pods drawn from the cluster")
          (is (every? (set pods) selected))
          (is (= (count selected) (count (distinct selected)))))))))

(deftest select-targets-nil-spec-test
  (testing "nil selects a non-empty random subset"
    (let [pods (pods 5)]
      (doseq [_trial (range 20)]
        (let [selected (exp/select-targets pods nil)]
          (is (<= 1 (count selected) 5)))))))

(deftest select-targets-randomizes-test
  (testing ":one eventually reaches every pod"
    (let [pods (pods 5)]
      (is (= (set pods)
             (set (mapcat (fn [_] (exp/select-targets pods :one))
                          (range 200))))))))

(deftest select-targets-explicit-collection-test
  (let [pods (pods 3)]
    (testing "an explicit collection keeps only pods that are still eligible"
      (is (= ["pod-0" "pod-2"]
             (exp/select-targets pods ["pod-0" "pod-2" "pod-gone"]))))
    (testing "a collection matching nothing eligible selects nothing"
      (is (= [] (exp/select-targets pods ["pod-gone"]))))
    (testing "an empty collection selects nothing"
      (is (= [] (exp/select-targets pods []))))))

(deftest select-targets-no-pods-test
  (testing "every spec selects nothing when the cluster has no pods"
    (doseq [spec [nil :one :minority :majority :minority-third :all
                  ["pod-0"] :nonsense]]
      (is (= [] (exp/select-targets [] spec)) (str spec " on an empty cluster")))))

(deftest select-targets-unknown-spec-test
  (testing "an unrecognized spec names the ones that are recognized"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"unknown target spec; expected nil, :one, :minority"
                          (exp/select-targets (pods 3) :most))))
  (testing "the error carries the offending spec and its type"
    (is (= {:spec :most :type clojure.lang.Keyword}
           (try (exp/select-targets (pods 3) :most)
                (catch clojure.lang.ExceptionInfo e (ex-data e)))))))
