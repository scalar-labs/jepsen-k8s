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
