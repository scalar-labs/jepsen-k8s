(defproject com.scalar-labs/jepsen-k8s-example-postgres-register "0.1.0-SNAPSHOT"
  :description "PostgreSQL register example for jepsen-k8s"
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.10"]
                 [com.scalar-labs/jepsen-k8s "0.1.0-SNAPSHOT"]
                 [org.postgresql/postgresql "42.7.4"]]
  :main jepsen.k8s.examples.postgres-register
  :source-paths ["src"]
  :test-paths ["test"])
