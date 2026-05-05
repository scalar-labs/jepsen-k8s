(ns jepsen.k8s.examples.postgres-register-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.nemesis :as nemesis]
            [jepsen.k8s.examples.postgres-register :as pg]))

(deftest test-map-test
  (let [t (pg/test {:nodes ["client-0" "client-1"]
                    :concurrency 2
                    :time-limit 5
                    :kube-context "kind-jepsen"
                    :namespace "jepsen-postgres"
                    :release "jepsen-postgres"
                    :chart "oci://registry-1.docker.io/bitnamicharts/postgresql"
                    :postgres-user "jepsen"
                    :postgres-password "jepsen"
                    :postgres-db "jepsen"
                    :postgres-port 5432})]
    (is (= "postgres-register" (:name t)))
    (is (= {:context "kind-jepsen"
            :namespace "jepsen-postgres"}
           (:k8s t)))
    (is (= {:dummy? true} (:ssh t)))
    (is (:db t))
    (is (:client t))))

(deftest chaos-mesh-nemesis-test
  (let [t (pg/test {:concurrency 2
                    :time-limit 5
                    :kube-context "kind-jepsen"
                    :namespace "jepsen-postgres"
                    :release "jepsen-postgres"
                    :chart "oci://registry-1.docker.io/bitnamicharts/postgresql"
                    :postgres-user "jepsen"
                    :postgres-password "jepsen"
                    :postgres-db "jepsen"
                    :postgres-port 5432
                    :nemesis [:kill]
                    :nemesis-interval 5})]
    (is (not= nemesis/noop (:nemesis t)))
    (is (:generator t))
    (is (:checker t))))
