(defproject com.scalar-labs/jepsen-k8s "0.1.0-SNAPSHOT"
  :description "Kubernetes helpers for Jepsen tests"
  :url "https://github.com/scalar-labs/jepsen-k8s"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.10"]
                 [clj-commons/clj-yaml "1.0.29"]
                 [cheshire "5.13.0"]]
  :plugins [[lein-cljfmt "0.9.2"]]
  :profiles {:dev {:dependencies [[lambdaisland/kaocha "1.91.1392"]]}}
  :source-paths ["src"]
  :test-paths ["test"])
