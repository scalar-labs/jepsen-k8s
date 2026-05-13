(ns jepsen.k8s.exec
  "Thin execution wrappers around kubectl and helm."
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [jepsen.control :as c]
            [jepsen.control.core :as cc]))

(defn- k8s-context-args
  "Returns kubectl/helm context args from test config."
  [test]
  (if-let [ctx (get-in test [:k8s :context])]
    [:--context ctx]
    []))

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
  "Runs kubectl with optional --context from test.

  Example:
    (kubectl! test :get :pod :-n \"jepsen-test\")"
  [test & args]
  (exec :kubectl (concat (k8s-context-args test) args)))

(defn helm!
  "Runs helm with optional --kube-context from test."
  [test & args]
  (let [ctx (get-in test [:k8s :context])
        ctx-args (if ctx [:--kube-context ctx] [])]
    (exec :helm (concat ctx-args args))))
