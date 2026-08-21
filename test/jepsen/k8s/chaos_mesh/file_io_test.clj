(ns jepsen.k8s.chaos-mesh.file-io-test
  (:require [clj-yaml.core :as yaml]
            [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.experiment :as exp]
            [jepsen.k8s.chaos-mesh.file-io :as file-io]
            [jepsen.k8s.core :as k8s]
            [jepsen.nemesis :as n]))

(def ^:private validate-config #'file-io/validate-config)
(def ^:private make-manifest #'file-io/make-manifest)
(def ^:private file-io-nemesis #'file-io/file-io-nemesis)

(def config
  {:volume-path "/var/lib/postgresql/data"
   :file-path "/var/lib/postgresql/data/pg_wal/**/*"
   :pod-selector {:app "postgres"}
   :container-names ["postgres"]
   :targets [:one]
   :methods [:read :write]
   :errno 5
   :percent 25})

(deftest config-validation-test
  (testing "defaults describe read and write EIO failures on one pod"
    (is (= {:targets [:one]
            :methods [:read :write]
            :errno 5
            :percent 100
            :volume-path "/data"
            :file-path "/data/**/*"}
           (validate-config {:volume-path "/data"
                             :file-path "/data/**/*"}))))
  (testing "file path must be inside the selected volume"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"file-path must be within volume-path"
                          (validate-config {:volume-path "/data"
                                            :file-path "/other/data"}))))
  (testing "only explicit read/write methods are accepted"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"methods must contain"
                          (validate-config {:volume-path "/data"
                                            :file-path "/data/file"
                                            :methods [:read :fsync]})))))

(defn- config-with-targets
  [targets]
  (validate-config {:volume-path "/data"
                    :file-path "/data/**/*"
                    :targets targets}))

(deftest target-validation-test
  (testing "the specs an op can be handed are accepted"
    (doseq [targets [[:one] [:all] [nil]
                     [:one :minority :majority :minority-third :all]
                     ;; A pod-name list has to be nested: an op is handed one
                     ;; element of :targets as its whole spec.
                     [["pod-0" "pod-1"]]
                     [:one ["pod-0"]]]]
      (is (= targets (:targets (config-with-targets targets))) (str targets))))

  (testing "a bare pod name is rejected at config time, not on the first op"
    ;; ["pod-0" "pod-1"] passes every shape check and then throws from
    ;; select-targets once the run is under way, because rand-nth hands it the
    ;; string "pod-0" rather than the list.
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"has to be nested"
                          (config-with-targets ["pod-0" "pod-1"]))))

  (testing "the error names just the offending elements"
    (is (= ["pod-0" 7]
           (try (config-with-targets [:one "pod-0" 7])
                (catch clojure.lang.ExceptionInfo e (:invalid (ex-data e))))))))

(deftest manifest-test
  (let [manifest (-> (make-manifest
                      {:k8s {:namespace "database"}}
                      ["postgres-1"]
                      (validate-config config))
                     yaml/parse-string)]
    (is (= "IOChaos" (:kind manifest)))
    (is (= {:name "file-io-fault" :namespace "chaos-mesh"}
           (:metadata manifest)))
    (is (= "fault" (get-in manifest [:spec :action])))
    (is (= "all" (get-in manifest [:spec :mode])))
    (is (= ["postgres-1"]
           (get-in manifest [:spec :selector :pods :database])))
    (is (= "/var/lib/postgresql/data"
           (get-in manifest [:spec :volumePath])))
    (is (= "/var/lib/postgresql/data/pg_wal/**/*"
           (get-in manifest [:spec :path])))
    (is (= ["READ" "WRITE"] (get-in manifest [:spec :methods])))
    (is (= ["postgres"] (get-in manifest [:spec :containerNames])))
    (is (= 5 (get-in manifest [:spec :errno])))
    (is (= 25 (get-in manifest [:spec :percent])))))

(deftest nemesis-selects-from-eligible-pods-test
  (let [pod-query (atom nil)
        applied   (atom nil)
        stops     (atom 0)
        nemesis   (file-io-nemesis (validate-config config) "/tmp")]
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
                               :f :start-file-io
                               :value ["postgres-1" "not-eligible"]})]
        (is (= {:selector {:app "postgres"}} @pod-query))
        (is (= ["postgres-1"] (get-in result [:value :targets])))
        (is (= ["postgres-1"]
               (get-in @applied
                       [:manifest :spec :selector :pods :database])))
        (is (= "/tmp" (:dir @applied)))
        (is (= 1 @stops))))))

(deftest nemesis-rejects-empty-selection-test
  (let [nemesis (file-io-nemesis (validate-config config) "/tmp")]
    (with-redefs [k8s/pod-names (fn [& _] [])]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"selected no eligible pods"
                            (n/invoke! nemesis
                                       {:k8s {:namespace "database"}}
                                       {:type :info
                                        :f :start-file-io
                                        :value :one}))))))

(defn- node-list
  [& archs]
  {:items (map-indexed (fn [i arch]
                         {:metadata {:name (str "node-" i)}
                          :status {:nodeInfo {:architecture arch}}})
                       archs)})

(deftest node-arch-check-test
  (let [nemesis (file-io-nemesis (validate-config config) "/tmp")
        setup!  (fn [nodes]
                  (with-redefs [k8s/nodes (fn [& _] nodes)
                                exp/stop! (fn [& _] nil)]
                    (n/setup! nemesis {:k8s {:namespace "database"}})))]
    (testing "an all-amd64 cluster sets up"
      (is (some? (setup! (node-list "amd64" "amd64" "amd64")))))
    (testing "any arm64 node stops the test before it runs"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"needs every node on amd64"
                            (setup! (node-list "amd64" "arm64")))))
    (testing "an unreported architecture is not assumed to be amd64"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"needs every node on amd64"
                            (setup! (node-list "amd64" nil)))))
    (testing "a cluster with no nodes stops the test too"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"found no nodes"
                            (setup! (node-list)))))
    (testing "the error names the offending nodes"
      (is (= {"node-1" "arm64"}
             (try (setup! (node-list "amd64" "arm64"))
                  (catch clojure.lang.ExceptionInfo e
                    (:unsupported-nodes (ex-data e)))))))))

(deftest node-arch-unchecked-without-the-fault-test
  (testing "a test that doesn't use the file I/O fault runs on any architecture"
    (let [package (file-io/file-io-package {:faults #{:kill} :dir "/tmp"})
          checked (atom false)]
      (with-redefs [k8s/nodes (fn [& _] (reset! checked true) (node-list "arm64"))
                    exp/stop! (fn [& _] nil)]
        (n/setup! (:nemesis package) {:k8s {:namespace "database"}}))
      (is (false? @checked)))))

(defn- record-lifecycle
  "Runs f with stop! and the node list stubbed, returning the calls each made
  in the order they happened."
  [nodes f]
  (let [calls (atom [])]
    (with-redefs [exp/stop! (fn [& _] (swap! calls conj :stop!) nil)
                  k8s/nodes (fn [& _] (swap! calls conj :get-nodes) nodes)]
      (f))
    @calls))

(deftest setup-clears-leftovers-before-checking-arch-test
  (let [nemesis (file-io-nemesis (validate-config config) "/tmp")
        setup!  #(n/setup! nemesis {:k8s {:namespace "database"}})]
    (testing "a leftover fault is cleared before the cluster is inspected"
      ;; jepsen derefs the setup future outside its try/finally, so a throw
      ;; here means teardown! never runs. Anything stop! would have removed
      ;; would stay applied for the rest of the cluster's life.
      (is (= [:stop! :get-nodes]
             (record-lifecycle (node-list "amd64") setup!))))
    (testing "including when the check then throws"
      (is (= [:stop! :get-nodes]
             (record-lifecycle
              (node-list "arm64")
              #(is (thrown? clojure.lang.ExceptionInfo (setup!)))))))))

(deftest lifecycle-untouched-without-the-fault-test
  (testing "a run that never asked for the fault issues no cleanup either way"
    (let [package (file-io/file-io-package {:faults #{:kill} :dir "/tmp"})
          nemesis (:nemesis package)
          test    {:k8s {:namespace "database"}}]
      (is (= [] (record-lifecycle (node-list "amd64") #(n/setup! nemesis test))))
      (is (= [] (record-lifecycle (node-list "amd64")
                                  #(n/teardown! nemesis test)))))))

(deftest teardown-clears-the-fault-test
  (testing "a run that did ask for it still cleans up"
    (let [nemesis (file-io-nemesis (validate-config config) "/tmp")]
      (is (= [:stop!]
             (record-lifecycle (node-list "amd64")
                               #(n/teardown! nemesis
                                             {:k8s {:namespace "database"}})))))))

(deftest package-test
  (let [package (file-io/file-io-package
                 {:faults #{:file-io}
                  :interval 10
                  :dir "/tmp"
                  :file-io config})]
    (is (some? (:generator package)))
    (is (= {:type :info :f :stop-file-io :value nil}
           (:final-generator package)))
    (is (= #{:start-file-io :stop-file-io}
           (set (n/fs (:nemesis package)))))))
