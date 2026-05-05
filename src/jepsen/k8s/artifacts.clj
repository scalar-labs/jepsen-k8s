(ns jepsen.k8s.artifacts
  "Run artifact collection helpers."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [jepsen.k8s.core :as k8s]))

(defn write-json!
  [path x]
  (.mkdirs (.getParentFile (io/file path)))
  (spit path (json/generate-string x {:pretty true})))

(defn collect-basic!
  "Collects a basic Kubernetes snapshot for a Jepsen run."
  [test {:keys [namespace selector output-dir]}]
  (let [ns (or namespace (k8s/namespace test))]
    (.mkdirs (io/file output-dir))
    (write-json! (str output-dir "/pods.json")
                 (k8s/pods test {:namespace ns :selector selector}))
    (write-json! (str output-dir "/services.json")
                 (k8s/services test {:namespace ns}))
    (write-json! (str output-dir "/nodes.json")
                 (k8s/nodes test))
    (write-json! (str output-dir "/events.json")
                 (k8s/events test {:namespace ns}))
    (k8s/collect-logs! test
                       {:namespace ns
                        :selector selector
                        :output-dir (str output-dir "/logs")})
    (k8s/collect-logs! test
                       {:namespace ns
                        :selector selector
                        :output-dir (str output-dir "/logs-previous")
                        :previous? true})))
