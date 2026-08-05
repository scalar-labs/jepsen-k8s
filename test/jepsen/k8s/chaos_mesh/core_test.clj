(ns jepsen.k8s.chaos-mesh.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.core :as cm]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.exec :as e]
            [jepsen.nemesis :as n]))

(defn- nodes-running
  "A node list as kubectl returns it, for nodes on the given runtimes."
  [& runtime-versions]
  {:items (mapv #(hash-map :status {:nodeInfo {:containerRuntimeVersion %}})
                runtime-versions)})

(deftest setup-test
  (let [calls (atom [])]
    (with-redefs [k8s/nodes (fn [_test] (nodes-running "containerd://2.2.0"))
                  e/helm! (fn [_test & args]
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
               :--set "chaosDaemon.runtime=containerd"
               :--set "chaosDaemon.socketPath=/run/containerd/containerd.sock"
               :--set "dashboard.securityMode=false"]]
             @calls)))))

(deftest setup-keeps-explicit-runtime-test
  (let [calls (atom [])]
    (with-redefs [k8s/nodes (fn [_test] (nodes-running "containerd://2.2.0"))
                  e/helm! (fn [_test & args]
                            (swap! calls conj (into [:helm] args))
                            "ok")]
      (cm/setup! {} {:set {:chaosDaemon.runtime "crio"
                           :chaosDaemon.socketPath "/var/run/crio/crio.sock"}})
      (is (= [:--set "chaosDaemon.runtime=crio"
              :--set "chaosDaemon.socketPath=/var/run/crio/crio.sock"]
             (->> (second @calls)
                  (drop-while #(not= :--set %))
                  vec))))))

(deftest detect-runtime-values-test
  (testing "maps the runtime Kubernetes reports to the daemon's Helm values"
    (doseq [[reported expected]
            {"containerd://2.2.0"
             {:chaosDaemon.runtime "containerd"
              :chaosDaemon.socketPath "/run/containerd/containerd.sock"}

             "cri-o://1.29.0"
             {:chaosDaemon.runtime "crio"
              :chaosDaemon.socketPath "/var/run/crio/crio.sock"}

             "docker://20.10.7"
             {:chaosDaemon.runtime "docker"
              :chaosDaemon.socketPath "/var/run/docker.sock"}}]
      (testing reported
        (with-redefs [k8s/nodes (fn [_test] (nodes-running reported))]
          (is (= expected (cm/detect-runtime-values {})))))))

  (testing "detects one runtime shared by every node"
    (with-redefs [k8s/nodes (fn [_test] (nodes-running "containerd://2.2.0"
                                                       "containerd://2.1.4"))]
      (is (= "containerd"
             (:chaosDaemon.runtime (cm/detect-runtime-values {}))))))

  (testing "keeps the chart default when the runtime is ambiguous"
    (with-redefs [k8s/nodes (fn [_test] (nodes-running "containerd://2.2.0"
                                                       "docker://20.10.7"))]
      (is (nil? (cm/detect-runtime-values {})))))

  (testing "keeps the chart default for an unknown runtime"
    (with-redefs [k8s/nodes (fn [_test] (nodes-running "mystery://1.0"))]
      (is (nil? (cm/detect-runtime-values {})))))

  (testing "keeps the chart default when the lookup fails"
    (with-redefs [k8s/nodes (fn [_test] (throw (ex-info "no cluster" {})))]
      (is (nil? (cm/detect-runtime-values {}))))))

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

(deftest file-io-package-test
  (let [package (cm/nemesis-package
                 nil
                 10
                 [:file-io]
                 {:file-io
                  {:volume-path "/var/lib/postgresql/data"
                   :file-path "/var/lib/postgresql/data/pg_wal/**/*"
                   :pod-selector {:app "postgres"}}})]
    (is (some? (:generator package)))
    (is (= #{:start-file-io :stop-file-io}
           (set (filter #{:start-file-io :stop-file-io}
                        (n/fs (:nemesis package))))))))
