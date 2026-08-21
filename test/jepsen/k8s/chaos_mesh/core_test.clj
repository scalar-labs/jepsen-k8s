(ns jepsen.k8s.chaos-mesh.core-test
  (:require [clj-yaml.core :as yaml]
            [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.core :as cm]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.exec :as e]
            [jepsen.nemesis :as n]))

(defn- nodes-running
  "A node list as kubectl returns it, for nodes on the given runtimes."
  [& runtime-versions]
  {:items (mapv #(hash-map :status {:nodeInfo {:containerRuntimeVersion %}})
                runtime-versions)})

(defn- flag-args
  "The values of one repeated flag in a recorded helm call, in order."
  [flag call]
  (->> (partition 2 1 call)
       (keep (fn [[f value]] (when (= flag f) value)))))

(def ^:private set-args (partial flag-args :--set))
(def ^:private values-args (partial flag-args :-f))

(defn- values-file
  "The parsed contents of a values file helm was pointed at."
  [path]
  (yaml/parse-string (slurp path)))

(defn- values-file!
  "A temp Helm values file holding m. Returns its path."
  [m]
  (let [file (doto (java.io.File/createTempFile "caller-" ".yaml")
               .deleteOnExit)]
    (spit file (yaml/generate-string m))
    (.getPath file)))

(defn- record-helm
  "Runs f with helm and the node list stubbed. Returns the recorded helm calls
  under :calls and how many times the nodes were listed under :lookups.

  runtime-versions is what the cluster's nodes report, or an exception to throw
  from the lookup."
  [runtime-versions f]
  (let [calls   (atom [])
        lookups (atom 0)]
    (with-redefs [k8s/nodes (fn [_test]
                              (swap! lookups inc)
                              (if (instance? Exception runtime-versions)
                                (throw runtime-versions)
                                (apply nodes-running runtime-versions)))
                  e/helm! (fn [_test & args]
                            (swap! calls conj (into [:helm] args))
                            "ok")]
      (f))
    {:calls @calls :lookups @lookups}))

(defn- install-call
  "The helm install call recorded by record-helm."
  [recorded]
  (second (:calls recorded)))

(deftest setup-test
  (let [recorded (record-helm
                  ["containerd://2.2.0"]
                  #(is (= "ok"
                          (cm/setup! {} {:version "2.7.2"
                                         :set {:dashboard.securityMode false}}))))
        install  (install-call recorded)
        detected (values-args install)]
    (is (= [:helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org"]
           (first (:calls recorded))))
    (testing "the detected runtime is passed as a values file, not --set"
      (is (= 1 (count detected)))
      (is (= {:chaosDaemon {:runtime "containerd"
                            :socketPath "/run/containerd/containerd.sock"}}
             (values-file (first detected))))
      (is (= ["dashboard.securityMode=false"] (set-args install))))
    (testing "the rest of the command is unchanged"
      (is (= [:helm :install "chaos-mesh" "chaos-mesh/chaos-mesh"
              :-n "chaos-mesh"
              :--create-namespace
              :--version "2.7.2"]
             (vec (take 9 install)))))))

(deftest setup-detected-values-come-first-test
  (testing "a caller's values file is passed after the detected one, so it wins"
    ;; The k3s case: the socket path a k3s node needs is not the one detection
    ;; infers from the containerd scheme, and before this it was unreachable
    ;; from a values file because --set beats -f whatever the flag order.
    (let [caller   (values-file!
                    {:chaosDaemon
                     {:socketPath "/run/k3s/containerd/containerd.sock"}})
          recorded (record-helm ["containerd://2.2.0"]
                                #(cm/setup! {} {:values [caller]}))
          files    (values-args (install-call recorded))]
      (is (= 2 (count files)))
      (is (= {:chaosDaemon {:runtime "containerd"
                            :socketPath "/run/containerd/containerd.sock"}}
             (values-file (first files))))
      (is (= caller (second files)))
      (testing "and nothing is emitted as --set to outrank it"
        (is (empty? (set-args (install-call recorded))))))))

(deftest setup-keeps-explicit-runtime-test
  (testing "a pinned runtime is used as-is, and detection is skipped entirely"
    (doseq [[channel opts]
            {":set"    {:set {:chaosDaemon.runtime "crio"
                              :chaosDaemon.socketPath "/var/run/crio/crio.sock"}}
             ":values" {:values [(values-file!
                                  {:chaosDaemon
                                   {:runtime "crio"
                                    :socketPath "/var/run/crio/crio.sock"}})]}}]
      (testing channel
        (let [recorded (record-helm ["containerd://2.2.0"]
                                    #(cm/setup! {} opts))
              install  (install-call recorded)]
          (testing "no detected values file is emitted"
            (is (= (count (:values opts)) (count (values-args install)))))
          (testing "the cluster is never asked"
            (is (= 0 (:lookups recorded)))))))))

(deftest setup-rejects-runtime-without-socket-path-test
  (testing "pinning the runtime alone would pair it with the detected socket"
    (doseq [[channel opts]
            {":set"    {:set {:chaosDaemon.runtime "crio"}}
             ":values" {:values [(values-file!
                                  {:chaosDaemon {:runtime "crio"}})]}}]
      (testing channel
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"requires chaosDaemon.socketPath"
             (record-helm ["containerd://2.2.0"] #(cm/setup! {} opts)))))))
  (testing "pinning the socket path alone is fine; it is the k3s workaround"
    (let [caller (values-file!
                  {:chaosDaemon
                   {:socketPath "/run/k3s/containerd/containerd.sock"}})]
      (is (= 2 (count (:calls (record-helm ["containerd://2.2.0"]
                                           #(cm/setup! {} {:values [caller]}))))))))
  (testing "an unreadable values file is left for helm to report"
    (is (= 2 (count (:calls (record-helm
                             ["containerd://2.2.0"]
                             #(cm/setup! {} {:values ["/nonexistent.yaml"]}))))))))

(deftest setup-requires-a-runtime-test
  (testing "an undetectable runtime stops the install rather than defaulting"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"couldn't determine the chaos-daemon container runtime"
         (record-helm ["mystery://1.0"] #(cm/setup! {} {})))))
  (testing "a failed lookup stops it too"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"couldn't determine the chaos-daemon container runtime"
         (record-helm (ex-info "no cluster" {}) #(cm/setup! {} {})))))
  (testing "but not when the caller pinned the runtime themselves"
    (let [recorded (record-helm
                    (ex-info "no cluster" {})
                    #(cm/setup!
                      {} {:set {:chaosDaemon.runtime "crio"
                                :chaosDaemon.socketPath "/var/run/crio/crio.sock"}}))]
      (is (= 2 (count (:calls recorded)))))))

(deftest setup-orders-set-args-test
  (testing "--set flags are sorted by key whatever order the map iterates in"
    ;; Ten keys, inserted in reverse. That is enough to spill past the
    ;; array-map threshold, so iteration order is neither the order they were
    ;; written in nor sorted order.
    (let [opts     (into {} (for [i (range 9 -1 -1)] [(keyword (str "k" i)) i]))
          recorded (record-helm ["containerd://2.2.0"]
                                #(cm/setup! {} {:set opts}))]
      (is (= ["k0=0" "k1=1" "k2=2" "k3=3" "k4=4"
              "k5=5" "k6=6" "k7=7" "k8=8" "k9=9"]
             (set-args (install-call recorded)))))))

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
