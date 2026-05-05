(ns jepsen.k8s.core
  "Generic Kubernetes operations for Jepsen tests."
  (:refer-clojure :exclude [namespace])
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.exec :as e]))

(defn namespace
  [test]
  (or (get-in test [:k8s :namespace]) "default"))

(defn kubectl!
  "Runs kubectl with this test's Kubernetes context.

  This is the primary escape hatch for Kubernetes operations. Prefer it for
  uncommon kubectl operations and CRDs; use the helpers below for Jepsen test
  patterns that are common enough to deserve names."
  [test & args]
  (apply e/kubectl! test args))

(defn kubectl-json!
  "Runs kubectl and parses the response as JSON."
  [test & args]
  (json/parse-string
   (apply kubectl! test (concat args [:-o :json]))
   true))

(defn kubectl-lines!
  "Runs kubectl and returns non-empty stdout lines."
  [test & args]
  (->> (apply kubectl! test args)
       str/split-lines
       (remove str/blank?)
       vec))

(defn label-selector
  "Converts a map or string into a kubectl label selector."
  [selector]
  (cond
    (nil? selector) nil
    (string? selector) selector
    (map? selector) (->> selector
                         (map (fn [[k v]] (str (name k) "=" v)))
                         (str/join ","))
    :else (throw (ex-info "Unsupported selector" {:selector selector}))))

(defn patch!
  [test resource name patch & args]
  (apply kubectl! test
         (concat [:patch resource name :--type :merge :-p patch]
                 args)))

(defn wait!
  [test {:keys [resource selector for timeout] ns :namespace}]
  (let [selector (label-selector selector)
        ns (or ns (namespace test))]
    (apply kubectl! test
           (concat [:wait resource :-n ns
                    :--for for]
                   (when selector [:-l selector])
                   [:--timeout (or timeout "300s")]))))

(defn pods
  [test {:keys [selector] ns :namespace}]
  (let [selector (label-selector selector)
        ns (or ns (namespace test))
        args (concat [:get :pod :-n ns]
                     (when selector [:-l selector]))]
    (apply kubectl-json! test args)))

(defn services
  [test {:keys [selector] ns :namespace}]
  (let [selector (label-selector selector)
        ns (or ns (namespace test))
        args (concat [:get :svc :-n ns]
                     (when selector [:-l selector]))]
    (apply kubectl-json! test args)))

(defn nodes
  [test]
  (kubectl-json! test :get :nodes))

(defn pod-names
  [test opts]
  (mapv #(get-in % [:metadata :name]) (:items (pods test opts))))

(defn pod-logs!
  [test {:keys [pod container previous? since] ns :namespace}]
  (apply kubectl! test
         (concat [:logs pod :-n (or ns (namespace test))]
                 (when container [:-c container])
                 (when previous? [:--previous])
                 (when since [:--since since]))))

(defn events
  [test {ns :namespace}]
  (kubectl-json! test :get :events :-n (or ns (namespace test))))

(defn collect-logs!
  "Collects logs for matching pods into output-dir."
  [test {:keys [namespace selector output-dir previous?]}]
  (.mkdirs (io/file output-dir))
  (doseq [pod (pod-names test {:namespace namespace :selector selector})]
    (let [path (io/file output-dir (str pod (when previous? ".previous") ".log"))]
      (try
        (spit path
              (pod-logs! test {:namespace namespace
                               :pod pod
                               :previous? previous?}))
        (catch Exception e
          (warn "skipping pod logs"
                {:pod pod
                 :namespace namespace
                 :previous? previous?
                 :error (.getMessage e)}))))))
