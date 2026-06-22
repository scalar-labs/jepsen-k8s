(ns jepsen.k8s.exec
  "Thin execution wrappers around kubectl and helm."
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [jepsen.control :as c]
            [jepsen.control.core :as cc]))

(defn- k8s-global-args
  "Returns kubectl/helm global args (kubeconfig and context) from test config.
  context-flag is :--context for kubectl and :--kube-context for helm; both
  tools accept --kubeconfig."
  [test context-flag]
  (let [{:keys [kubeconfig context]} (:k8s test)]
    (concat (when kubeconfig [:--kubeconfig kubeconfig])
            (when context [context-flag context]))))

(defn- exec
  [& commands]
  (->> commands
       (map cc/escape)
       (str/join " ")
       c/wrap-trace
       (sh "sh" "-c")
       cc/throw-on-nonzero-exit
       c/just-stdout))

(defn kubectl!
  "Runs kubectl with optional --kubeconfig and --context from test.

  Example:
    (kubectl! test :get :pod :-n \"jepsen-test\")"
  [test & args]
  (exec :kubectl (concat (k8s-global-args test :--context) args)))

(defn helm!
  "Runs helm with optional --kubeconfig and --kube-context from test."
  [test & args]
  (exec :helm (concat (k8s-global-args test :--kube-context) args)))
