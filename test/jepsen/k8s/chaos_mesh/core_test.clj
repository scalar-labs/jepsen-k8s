(ns jepsen.k8s.chaos-mesh.core-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.k8s.chaos-mesh.core :as cm]
            [jepsen.k8s.exec :as e]))

(deftest setup-test
  (let [calls (atom [])]
    (with-redefs [e/helm! (fn [_test & args]
                            (swap! calls conj (into [:helm] args))
                            "ok")]
      (is (= "ok"
             (cm/setup! {} {:version "2.7.2"
                            :set {:dashboard.securityMode false}})))
      (is (= [[:helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org"]
              [:helm :install "chaos-mesh" "chaos-mesh/chaos-mesh"
               :-n "chaos-mesh"
               :--create-namespace
               :--version "2.7.2"
               :--set "dashboard.securityMode=false"]]
             @calls)))))

(deftest wipe-test
  (let [calls (atom [])]
    (with-redefs [e/helm! (fn [_test & args]
                            (swap! calls conj (vec args))
                            "ok")]
      (is (= "ok"
             (cm/wipe! {} {:timeout "60s"})))
      (is (= [[:uninstall "chaos-mesh"
               :-n "chaos-mesh"
               :--timeout "60s"
               :--ignore-not-found]]
             @calls)))))
