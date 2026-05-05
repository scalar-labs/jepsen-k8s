(ns jepsen.k8s.examples.postgres-register
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.k8s.artifacts :as artifacts]
            [jepsen.k8s.chaos-mesh.core :as chaos-mesh]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [jepsen.k8s.nemesis :as k8s-nemesis]
            [jepsen.tests.linearizable-register :as register])
  (:import (java.sql DriverManager SQLException)))

(def default-chart
  "oci://registry-1.docker.io/bitnamicharts/postgresql")

(def default-release "jepsen-postgres")
(def default-namespace "jepsen-postgres")
(def default-db "jepsen")
(def default-user "jepsen")
(def default-password "jepsen")
(def default-postgres-port 5432)
(def default-nemesis-interval 30)

(def supported-nemeses
  #{:pause :kill :partition :packet :clock})

(defn- parse-nemesis
  [s]
  (let [fault (keyword s)]
    (when-not (supported-nemeses fault)
      (throw (IllegalArgumentException.
              (str "Expected one of "
                   (str/join ", " (sort (map name supported-nemeses)))
                   ", got " (pr-str s)))))
    fault))

(defn- service-name
  [{:keys [release service]}]
  (or service (str release "-postgresql")))

(defn- jdbc-url
  [{:keys [jdbc-url jdbc-url-ref postgres-db]}]
  (or jdbc-url
      @jdbc-url-ref
      (throw (ex-info "JDBC URL has not been initialized" {}))))

(defn- load-balancer-host
  [svc]
  (some-> svc
          (get-in [:status :loadBalancer :ingress])
          first
          ((fn [ingress] (or (:ip ingress) (:hostname ingress))))))

(defn- wait-for-load-balancer!
  [test {:keys [namespace] :as opts}]
  (let [service (service-name opts)
        deadline (+ (System/currentTimeMillis) 300000)]
    (loop []
      (let [svc (k8s/kubectl-json! test :get :svc service :-n namespace)
            host (load-balancer-host svc)]
        (if host
          host
          (do
            (when (<= deadline (System/currentTimeMillis))
              (throw (ex-info "Timed out waiting for LoadBalancer ingress"
                              {:namespace namespace
                               :service service})))
            (Thread/sleep 1000)
            (recur)))))))

(declare open-connection!)

(defn- wait-for-postgres!
  [opts timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [success? (try
                       (with-open [_ (open-connection! opts)]
                         true)
                       (catch Exception _ false))]
        (if success?
          true
          (do
            (when (<= deadline (System/currentTimeMillis))
              (throw (ex-info "Timed out waiting for PostgreSQL"
                              {:jdbc-url (jdbc-url opts)})))
            (Thread/sleep 500)
            (recur)))))))

(defn- ignore-conflict
  [f]
  (try
    (f)
    (catch Exception _ nil)))

(declare execute!)

(defn- initialize-schema!
  [opts]
  (Class/forName "org.postgresql.Driver")
  (with-open [conn (DriverManager/getConnection
                    (jdbc-url opts)
                    (:postgres-user opts)
                    (:postgres-password opts))]
    (execute! conn
              "create table if not exists registers (id bigint primary key, value bigint)"
              [])))

(defn- setup-chaos-mesh!
  [test opts]
  (when (seq (:nemesis opts))
    (chaos-mesh/setup! test {})))

(defn- wipe-chaos-mesh!
  [test opts]
  (when (seq (:nemesis opts))
    (chaos-mesh/wipe! test {})))

(defn- wipe!
  [test opts]
  (let [{:keys [namespace release]} opts]
    (ignore-conflict #(helm/uninstall! test {:release release
                                             :namespace namespace
                                             :timeout "300s"
                                             :ignore-not-found? true}))
    (ignore-conflict #(k8s/kubectl! test :delete :namespace namespace
                                    :--ignore-not-found=true))
    (ignore-conflict #(wipe-chaos-mesh! test opts))))

(defrecord PostgresDB [opts installed?]
  db/DB
  (setup! [_ test _node]
    (locking installed?
      (when-not @installed?
        (wipe! test opts)
        (setup-chaos-mesh! test opts)
        (let [{:keys [namespace release chart values postgres-user
                      postgres-password postgres-db postgres-port jdbc-url-ref]} opts]
          (ignore-conflict #(k8s/kubectl! test :create :namespace namespace))
          (helm/install! test {:release release
                               :chart chart
                               :namespace namespace
                               :values values
                               :set {:auth.username postgres-user
                                     :auth.password postgres-password
                                     :auth.database postgres-db
                                     :primary.service.type "LoadBalancer"
                                     :primary.persistence.enabled true}
                               :wait? true
                               :timeout "300s"})
          (k8s/wait! test {:namespace namespace
                           :resource :pod
                           :selector {"app.kubernetes.io/instance" release}
                           :for "condition=Ready"
                           :timeout "300s"})
          (when-not (:jdbc-url opts)
            (let [host (wait-for-load-balancer! test opts)]
              (reset! jdbc-url-ref
                      (str "jdbc:postgresql://" host ":" postgres-port "/" postgres-db))))
          (wait-for-postgres! opts 300000)
          (initialize-schema! opts)
          (reset! installed? true)))))

  (teardown! [_ test _node]
    (locking installed?
      (when @installed?
        (let [{:keys [namespace release]} opts]
          (artifacts/collect-basic!
           test
           {:namespace namespace
            :selector {"app.kubernetes.io/instance" release}
            :output-dir "store/postgres-register/k8s"})
          (when-not (:leave-db-running? test)
            (wipe! test opts))
          (reset! installed? false)))))

  db/Kill
  (kill! [_ _test _node])
  (start! [_ _test _node])

  db/Pause
  (pause! [_ _test _node])
  (resume! [_ _test _node]))

(defn db
  [opts]
  (->PostgresDB opts (atom false)))

(defn- execute!
  [conn sql params]
  (with-open [stmt (.prepareStatement conn sql)]
    (doseq [[i param] (map-indexed vector params)]
      (.setObject stmt (inc i) param))
    (.executeUpdate stmt)))

(defn- query-one
  [conn sql params]
  (with-open [stmt (.prepareStatement conn sql)]
    (doseq [[i param] (map-indexed vector params)]
      (.setObject stmt (inc i) param))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(defn- open-connection!
  [opts]
  (Class/forName "org.postgresql.Driver")
  (let [conn (DriverManager/getConnection
              (jdbc-url opts)
              (:postgres-user opts)
              (:postgres-password opts))]
    (.setAutoCommit conn true)
    conn))

(defn- close-connection!
  [conn-ref]
  (when-let [conn @conn-ref]
    (reset! conn-ref nil)
    (try
      (.close conn)
      (catch SQLException _))))

(defn- connection!
  [_test opts conn-ref]
  (or @conn-ref
      (let [conn (open-connection! opts)]
        (reset! conn-ref conn)
        conn)))

(defrecord RegisterClient [opts conn-ref]
  client/Client
  (open! [_ _test _node]
    (RegisterClient. opts (atom (open-connection! opts))))

  (setup! [this _test]
    this)

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (try
        (let [conn (connection! test opts conn-ref)]
          (case (:f op)
            :write
            (do
              (execute! conn
                        (str "insert into registers (id, value) values (?, ?) "
                             "on conflict (id) do update set value = excluded.value")
                        [k v])
              (assoc op :type :ok))

            :read
            (let [value (query-one conn "select value from registers where id = ?" [k])]
              (assoc op :type :ok :value (independent/tuple k value)))

            :cas
            (let [[expected new-value] v
                  updated (execute! conn
                                    (str "update registers set value = ? "
                                         "where id = ? and value = ?")
                                    [new-value k expected])]
              (assoc op :type (if (= 1 updated) :ok :fail)))))
        (catch SQLException e
          (close-connection! conn-ref)
          (assoc op :type :fail :error (.getMessage e)))
        (catch Exception e
          (close-connection! conn-ref)
          (assoc op :type :fail :error (.getMessage e))))))

  (teardown! [_ _test])

  (close! [_ _test]
    (close-connection! conn-ref)))

(defn client
  [opts]
  (RegisterClient. opts (atom nil)))

(def cli-opts
  [[nil "--kube-context CONTEXT" "Kubernetes context"]
   [nil "--namespace NAMESPACE" "Kubernetes namespace"
    :default default-namespace]
   [nil "--release RELEASE" "Helm release name"
    :default default-release]
   [nil "--chart CHART" "Helm chart reference"
    :default default-chart]
   [nil "--values FILE" "Helm values file; may be passed more than once"
    :default []
    :assoc-fn (fn [m k v] (update m k conj v))]
   [nil "--service SERVICE" "PostgreSQL service name; defaults to <release>-postgresql"]
   [nil "--postgres-user USER" "PostgreSQL user"
    :default default-user]
   [nil "--postgres-password PASSWORD" "PostgreSQL password"
    :default default-password]
   [nil "--postgres-db DB" "PostgreSQL database"
    :default default-db]
   [nil "--jdbc-url URL" "JDBC URL; skips LoadBalancer address discovery"]
   [nil "--postgres-port PORT" "PostgreSQL service port"
    :default default-postgres-port
    :parse-fn #(Long/parseLong %)]
   [nil "--nemesis FAULT"
    "Chaos Mesh nemesis fault; repeat for multiple faults. One of pause, kill, partition, packet, clock."
    :default []
    :parse-fn parse-nemesis
    :assoc-fn (fn [m k v] (update m k conj v))]
   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations"
    :default default-nemesis-interval
    :parse-fn #(Long/parseLong %)]])

(defn normalize-opts
  [opts]
  (update opts :jdbc-url-ref #(or % (atom nil))))

(defn- generator
  [workload nemesis opts]
  (gen/phases
   (->> workload
        (gen/nemesis
         (gen/phases
          (gen/sleep 5)
          (:generator nemesis)))
        (gen/time-limit (:time-limit opts)))
   (gen/nemesis (:final-generator nemesis))))

(defn test
  [opts]
  (let [opts (normalize-opts opts)
        ;; we only care about the count of nodes for this test
        nodes ["client-0" "client-1"]
        pg-db (db opts)
        faults (seq (:nemesis opts))
        package (when faults
                  (k8s-nemesis/chaos-mesh-package
                   pg-db
                   (:nemesis-interval opts)
                   faults))
        register-test (register/test (assoc opts :nodes nodes))]
    (merge tests/noop-test
           register-test
           {:name "postgres-register"
            :nodes nodes
            :ssh {:dummy? true}
            :db pg-db
            :client (client opts)
            :nemesis (or (:nemesis package) nemesis/noop)
            :k8s {:context (:kube-context opts)
                  :namespace (:namespace opts)}
            :concurrency (* 2 (count nodes))
            :generator (generator (:generator register-test) package opts)
            :checker (checker/compose
                      {:register (:checker register-test)
                       :perf (checker/perf
                              (cond-> {}
                                package
                                (assoc :nemeses (:perf package))))})})))

(defn -main
  [& args]
  (cli/run!
   (cli/single-test-cmd
    {:test-fn test
     :opt-spec cli-opts})
   args))
