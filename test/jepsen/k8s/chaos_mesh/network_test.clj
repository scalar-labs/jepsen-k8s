(ns jepsen.k8s.chaos-mesh.network-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-yaml.core :as yaml]
            [jepsen.k8s.chaos-mesh.network :as network]))

(def ^:private pod-grudge #'network/pod-grudge)
(def ^:private grudge->rules #'network/grudge->rules)
(def ^:private make-partition-manifest #'network/make-partition-manifest)
(def ^:private pod-targets #'network/pod-targets)

(def pods ["pg-0" "pg-1" "pg-2"])

(deftest pod-grudge-keys-are-pods
  (testing ":one isolates a single pod from the rest"
    (let [grudge (pod-grudge pods :one)]
      (is (= (set pods) (set (keys grudge))))
      ;; exactly one pod is cut off from both others
      (let [[loner cut] (first (filter (fn [[_ v]] (= 2 (count v))) grudge))]
        (is (= (disj (set pods) loner) cut))
        ;; the others can only not reach the loner
        (doseq [[pod v] (dissoc grudge loner)]
          (is (= #{loner} v) (str pod)))))))

(deftest grudge->rules-mirrors-drop-all
  (testing "each node drops inbound traffic from exactly its grudge set"
    (let [grudge (pod-grudge pods :one)
          rules  (grudge->rules grudge)
          ;; reconstruct, for every node, the set it should drop from
          reconstructed (into {}
                              (for [{:keys [nodes sources]} rules
                                    node nodes]
                                [node (set sources)]))]
      (is (= grudge reconstructed)))))

(deftest grudge->rules-handles-asymmetric-grudge
  (testing "an asymmetric grudge yields per-node rules, no info lost"
    (let [grudge {"a" #{"b"}        ; a drops from b, but b does not drop from a
                  "b" #{}
                  "c" #{"a" "b"}}
          rules  (grudge->rules grudge)
          reconstructed (into {}
                              (for [{:keys [nodes sources]} rules
                                    node nodes]
                                [node (set sources)]))]
      ;; empty drop-sets produce no rule
      (is (= {"a" #{"b"} "c" #{"a" "b"}} reconstructed)))))

(deftest partition-manifest-blocks-sources-into-nodes
  (let [test {:k8s {:namespace "jepsen-postgres"}}
        manifest (yaml/parse-string
                  (make-partition-manifest test "partition-0"
                                           ["pg-0"] ["pg-1" "pg-2"]))
        sel (get-in manifest [:spec :selector :pods :jepsen-postgres])
        tgt (get-in manifest [:spec :target :selector :pods :jepsen-postgres])]
    (is (= "partition" (get-in manifest [:spec :action])))
    ;; :from blocks target -> selector, i.e. drops inbound on the selector node
    (is (= "from" (get-in manifest [:spec :direction])))
    (is (= ["pg-0"] sel))
    (is (= ["pg-1" "pg-2"] tgt))
    ;; labelled so the whole partition can be torn down together
    (is (= "true" (get-in manifest [:metadata :labels :jepsen-partition])))
    (is (= "chaos-mesh" (get-in manifest [:metadata :namespace])))))

(deftest grudge->rules-is-deterministic
  (testing "nodes, sources, and rule order are stable for a given grudge"
    (let [grudge {"pg-2" #{"pg-1" "pg-0"}
                  "pg-1" #{"pg-0"}
                  "pg-0" #{"pg-1"}}]
      (is (= (grudge->rules grudge) (grudge->rules grudge)))
      (is (= [{:nodes ["pg-0"]        :sources ["pg-1"]}
              {:nodes ["pg-1"]        :sources ["pg-0"]}
              {:nodes ["pg-2"]        :sources ["pg-0" "pg-1"]}]
             (grudge->rules grudge))))))

(deftest pod-targets-respects-spec
  (is (= 1 (count (pod-targets pods :one))))
  (is (every? (set pods) (pod-targets pods :one)))
  (is (= (set pods) (set (pod-targets pods :all)))))

(deftest empty-pods-do-not-throw
  (testing "no pods yields an empty grudge / selection instead of throwing"
    (doseq [spec [:one :majority :majorities-ring :minority-third]]
      (is (= {} (pod-grudge [] spec)) (str spec)))
    (doseq [spec [nil :one :minority :majority :minority-third :all]]
      (is (= [] (pod-targets [] spec)) (str spec)))))
