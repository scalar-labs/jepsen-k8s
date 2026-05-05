(ns jepsen.k8s.helm-test
  (:require [clojure.test :refer :all]
            [jepsen.k8s.exec :as e]
            [jepsen.k8s.helm :as helm]))

(deftest install-test
  (let [calls (atom [])]
    (with-redefs [e/helm! (fn [test & args]
                            (swap! calls conj {:test test :args (vec args)})
                            "installed")]
      (is (= "installed"
             (helm/install! {:k8s {:context "kind-jepsen"}}
                            {:release "db"
                             :chart "./chart"
                             :namespace "jepsen-test"
                             :version "1.2.3"
                             :values ["values.yaml" "ci.yaml"]
                             :set {:replicas 3}
                             :wait? true
                             :timeout "300s"})))
      (is (= [{:test {:k8s {:context "kind-jepsen"}}
               :args [:install "db" "./chart"
                      :-n "jepsen-test"
                      :--create-namespace
                      :--version "1.2.3"
                      :-f "values.yaml"
                      :-f "ci.yaml"
                      :--set "replicas=3"
                      :--wait
                      :--timeout "300s"]}]
             @calls)))))

(deftest uninstall-test
  (let [calls (atom [])]
    (with-redefs [e/helm! (fn [_test & args]
                            (swap! calls conj (vec args))
                            "uninstalled")]
      (is (= "uninstalled"
             (helm/uninstall! {} {:release "db"
                                  :namespace "jepsen-test"
                                  :timeout "120s"
                                  :ignore-not-found? true})))
      (is (= [[:uninstall "db"
               :-n "jepsen-test"
               :--timeout "120s"
               :--ignore-not-found]]
             @calls)))))

(deftest template-test
  (let [calls (atom [])]
    (with-redefs [e/helm! (fn [_test & args]
                            (swap! calls conj (vec args))
                            "---")]
      (is (= "---"
             (helm/template {} {:release "db"
                                :chart "./chart"
                                :set {:image.tag "latest"}})))
      (is (= [[:template "db" "./chart" :--set "image.tag=latest"]]
             @calls)))))
