(ns jepsen.k8s.core-test
  (:require [clojure.test :refer [deftest is]]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [jepsen.k8s.exec :as e]
            [jepsen.k8s.core :as k8s]))

(deftest label-selector-test
  (is (= "app=db,tier=storage" (k8s/label-selector {:app "db" :tier "storage"})))
  (is (= "app=db" (k8s/label-selector "app=db")))
  (is (nil? (k8s/label-selector nil))))

(deftest kubectl-json-test
  (let [calls (atom [])]
    (with-redefs [e/kubectl! (fn [test & args]
                               (swap! calls conj {:test test :args (vec args)})
                               (json/generate-string {:items [{:metadata {:name "pod-0"}}]}))]
      (is (= {:items [{:metadata {:name "pod-0"}}]}
             (k8s/kubectl-json! {:k8s {:context "kind-jepsen"}}
                                :get :pod :-n "jepsen-test")))
      (is (= [{:test {:k8s {:context "kind-jepsen"}}
               :args [:get :pod :-n "jepsen-test" :-o :json]}]
             @calls)))))

(deftest discovery-helper-test
  (let [calls (atom [])]
    (with-redefs [e/kubectl! (fn [_test & args]
                               (swap! calls conj (vec args))
                               (json/generate-string
                                {:items [{:metadata {:name "db-0"}}
                                         {:metadata {:name "db-1"}}]}))]
      (is (= ["db-0" "db-1"]
             (k8s/pod-names {:k8s {:namespace "jepsen-test"}}
                            {:selector {:app "db"}})))
      (is (= [[:get :pod :-n "jepsen-test" :-l "app=db" :-o :json]]
             @calls)))))

(deftest wait-test
  (let [calls (atom [])]
    (with-redefs [e/kubectl! (fn [_test & args]
                               (swap! calls conj (vec args))
                               "pod/db-0 condition met")]
      (is (= "pod/db-0 condition met"
             (k8s/wait! {:k8s {:namespace "jepsen-test"}}
                        {:resource :pod
                         :selector {:app "db"}
                         :for "condition=Ready"})))
      (is (= [[:wait :pod :-n "jepsen-test"
               :--for "condition=Ready"
               :-l "app=db"
               :--timeout "300s"]]
             @calls)))))

(deftest pod-logs-test
  (let [calls (atom [])]
    (with-redefs [e/kubectl! (fn [_test & args]
                               (swap! calls conj (vec args))
                               "log line")]
      (is (= "log line"
             (k8s/pod-logs! {:k8s {:namespace "jepsen-test"}}
                            {:pod "db-0"
                             :container "db"
                             :previous? true
                             :since "10m"})))
      (is (= [[:logs "db-0" :-n "jepsen-test"
               :-c "db"
               :--previous
               :--since "10m"]]
             @calls)))))

(defn- pod
  ([name containers] (pod name containers nil))
  ([name containers init-containers]
   {:metadata {:name name}
    :spec (cond-> {:containers (mapv #(hash-map :name %) containers)}
            init-containers (assoc :initContainers
                                   (mapv #(hash-map :name %) init-containers)))}))

(defn- logs-container
  "The container name a (:logs ...) call asked for, found by scanning for the
  flag rather than by position so optional args can't shift it out from under
  us."
  [args]
  (second (drop-while #(not= :-c %) args)))

(defn- clean-dir!
  [name]
  (let [dir (doto (io/file "target" name) (.mkdirs))]
    (doseq [file (file-seq dir)
            :when (.isFile file)]
      (.delete file))
    dir))

(deftest collect-logs-skips-missing-logs-test
  (let [dir (clean-dir! "collect-logs-test")
        calls (atom [])]
    (with-redefs [e/kubectl! (fn [_test & args]
                               (swap! calls conj (vec args))
                               (case (first args)
                                 :get
                                 (json/generate-string
                                  {:items [(pod "db-0" ["db"])
                                           (pod "db-1" ["db"])]})

                                 :logs
                                 (if (= "db-0" (second args))
                                   "db-0 log"
                                   (throw (ex-info "previous container not found"
                                                   {:pod (second args)})))))]
      (is (nil? (k8s/collect-logs! {:k8s {:namespace "jepsen-test"}}
                                   {:selector {:app "db"}
                                    :output-dir (.getPath dir)
                                    :previous? true})))
      (is (= "db-0 log"
             (slurp (io/file dir "db-0.db.previous.log"))))
      (is (not (.exists (io/file dir "db-1.db.previous.log"))))
      (is (= [[:get :pod :-n "jepsen-test" :-l "app=db" :-o :json]
              [:logs "db-0" :-n "jepsen-test" :-c "db" :--previous]
              [:logs "db-1" :-n "jepsen-test" :-c "db" :--previous]]
             @calls)))))

(deftest collect-logs-collects-every-container-test
  (let [dir (clean-dir! "collect-logs-containers-test")
        calls (atom [])]
    ;; A pod with sidecars and an init container: kubectl logs without -c would
    ;; fail on it, taking the database's own log down with it. One container
    ;; fails here to pin down that a failure is scoped to its own container and
    ;; doesn't drop its siblings.
    (with-redefs [e/kubectl! (fn [_test & args]
                               (swap! calls conj (vec args))
                               (case (first args)
                                 :get
                                 (json/generate-string
                                  {:items [(pod "db-0"
                                                ["db" "logrotate" "ui"]
                                                ["init-chmod-data"])]})

                                 :logs
                                 (let [container (logs-container args)]
                                   (if (= "logrotate" container)
                                     (throw (ex-info "container not found"
                                                     {:container container}))
                                     (str (second args) " " container " log")))))]
      (is (nil? (k8s/collect-logs! {:k8s {:namespace "jepsen-test"}}
                                   {:selector {:app "db"}
                                    :output-dir (.getPath dir)})))
      (is (= "db-0 db log" (slurp (io/file dir "db-0.db.log"))))
      (is (= "db-0 ui log" (slurp (io/file dir "db-0.ui.log"))))
      (is (= "db-0 init-chmod-data log"
             (slurp (io/file dir "db-0.init-chmod-data.log"))))
      (is (not (.exists (io/file dir "db-0.logrotate.log"))))
      (is (= [[:get :pod :-n "jepsen-test" :-l "app=db" :-o :json]
              [:logs "db-0" :-n "jepsen-test" :-c "db"]
              [:logs "db-0" :-n "jepsen-test" :-c "logrotate"]
              [:logs "db-0" :-n "jepsen-test" :-c "ui"]
              [:logs "db-0" :-n "jepsen-test" :-c "init-chmod-data"]]
             @calls)))))

(deftest collect-logs-survives-a-failed-listing-test
  (let [dir (clean-dir! "collect-logs-listing-test")]
    (with-redefs [e/kubectl! (fn [_test & _args]
                               (throw (ex-info "connection refused" {})))]
      (is (nil? (k8s/collect-logs! {:k8s {:namespace "jepsen-test"}}
                                   {:selector {:app "db"}
                                    :output-dir (.getPath dir)}))))))
