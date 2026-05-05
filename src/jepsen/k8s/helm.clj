(ns jepsen.k8s.helm
  "Helm helpers. Initially wraps helm install/uninstall; render/template can be
  used later to move toward template -> kubectl apply."
  (:require [jepsen.k8s.exec :as e]))

(defn helm!
  "Runs helm with this test's Kubernetes context."
  [test & args]
  (apply e/helm! test args))

(defn repo-add!
  [test name url]
  (helm! test :repo :add name url))

(defn repo-update!
  [test]
  (helm! test :repo :update))

(defn install!
  [test {:keys [release chart namespace version values set wait? timeout]}]
  (apply helm! test
         (concat [:install release chart]
                 (when namespace [:-n namespace :--create-namespace])
                 (when version [:--version version])
                 (mapcat #(vector :-f %) values)
                 (mapcat (fn [[k v]] [:--set (str (name k) "=" v)]) set)
                 (when wait? [:--wait])
                 (when timeout [:--timeout timeout]))))

(defn uninstall!
  [test {:keys [release namespace timeout ignore-not-found?]}]
  (apply helm! test
         (concat [:uninstall release]
                 (when namespace [:-n namespace])
                 (when timeout [:--timeout timeout])
                 (when ignore-not-found? [:--ignore-not-found]))))

(defn template
  "Renders a chart to YAML string. Intended for future template -> apply flows."
  [test {:keys [release chart namespace values set]}]
  (apply helm! test
         (concat [:template release chart]
                 (when namespace [:-n namespace])
                 (mapcat #(vector :-f %) values)
                 (mapcat (fn [[k v]] [:--set (str (name k) "=" v)]) set))))
