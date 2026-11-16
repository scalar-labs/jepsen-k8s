(ns jepsen.k8s.chaos-mesh.stress-probe
  "Observes StressChaos effects through container cgroup counters."
  (:require [clojure.string :as str]
            [jepsen.k8s.chaos-mesh.probe :as probe]
            [jepsen.k8s.core :as k8s]))

(def ^:private cgroup-script
  (str
   "section() { printf '[%s]\\n' \"$1\"; }\n"
   "if [ -r /sys/fs/cgroup/cgroup.controllers ]; then\n"
   "  section cgroup.version; printf '2\\n'\n"
   "  section cpu.stat; cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true\n"
   "  section cpu.pressure; cat /sys/fs/cgroup/cpu.pressure 2>/dev/null || true\n"
   "  section memory.current; cat /sys/fs/cgroup/memory.current 2>/dev/null || true\n"
   "  section memory.max; cat /sys/fs/cgroup/memory.max 2>/dev/null || true\n"
   "  section memory.events; cat /sys/fs/cgroup/memory.events 2>/dev/null || true\n"
   "  section memory.pressure; cat /sys/fs/cgroup/memory.pressure 2>/dev/null || true\n"
   "else\n"
   "  section cgroup.version; printf '1\\n'\n"
   "  section cpu.usage-ns\n"
   "  cat /sys/fs/cgroup/cpuacct/cpuacct.usage 2>/dev/null || "
   "cat /sys/fs/cgroup/cpuacct.usage 2>/dev/null || true\n"
   "  section cpu.stat\n"
   "  cat /sys/fs/cgroup/cpu/cpu.stat 2>/dev/null || "
   "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true\n"
   "  section memory.current\n"
   "  cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || "
   "cat /sys/fs/cgroup/memory.usage_in_bytes 2>/dev/null || true\n"
   "  section memory.max\n"
   "  cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || "
   "cat /sys/fs/cgroup/memory.limit_in_bytes 2>/dev/null || true\n"
   "  section memory.failcnt\n"
   "  cat /sys/fs/cgroup/memory/memory.failcnt 2>/dev/null || "
   "cat /sys/fs/cgroup/memory.failcnt 2>/dev/null || true\n"
   "fi\n"))

(defn- parse-value
  [value]
  (cond
    (= "max" value) :max
    (re-matches #"-?\d+" value) (Long/parseLong value)
    (re-matches #"-?\d+\.\d+" value) (Double/parseDouble value)
    :else value))

(defn- sections
  [output]
  (:values
   (reduce
    (fn [{:keys [section] :as acc} line]
      (if-let [[_ name] (re-matches #"\[(.+)\]" line)]
        (assoc acc :section name)
        (if (or (nil? section) (str/blank? line))
          acc
          (update-in acc [:values section] (fnil conj []) line))))
    {:section nil :values {}}
    (str/split-lines output))))

(defn- scalar
  [sections name]
  (some-> (get sections name) first parse-value))

(defn- key-values
  [lines]
  (into {}
        (keep (fn [line]
                (let [[key value] (str/split line #"\s+" 2)]
                  (when value
                    [(keyword key) (parse-value value)]))))
        lines))

(defn- pressure
  [lines]
  (into {}
        (keep (fn [line]
                (let [[scope & values] (str/split line #"\s+")]
                  (when scope
                    [(keyword scope)
                     (into {}
                           (keep (fn [value]
                                   (let [[key n] (str/split value #"=" 2)]
                                     (when n
                                       [(keyword key) (parse-value n)]))))
                           values)]))))
        lines))

(defn- parse-cgroup-output
  [output]
  (let [s (sections output)]
    (cond-> {:version (scalar s "cgroup.version")}
      (seq (get s "cpu.stat"))
      (assoc-in [:cpu :stat] (key-values (get s "cpu.stat")))

      (some? (scalar s "cpu.usage-ns"))
      (assoc-in [:cpu :usage-ns] (scalar s "cpu.usage-ns"))

      (seq (get s "cpu.pressure"))
      (assoc-in [:cpu :pressure] (pressure (get s "cpu.pressure")))

      (some? (scalar s "memory.current"))
      (assoc-in [:memory :current] (scalar s "memory.current"))

      (some? (scalar s "memory.max"))
      (assoc-in [:memory :max] (scalar s "memory.max"))

      (seq (get s "memory.events"))
      (assoc-in [:memory :events] (key-values (get s "memory.events")))

      (some? (scalar s "memory.failcnt"))
      (assoc-in [:memory :failcnt] (scalar s "memory.failcnt"))

      (seq (get s "memory.pressure"))
      (assoc-in [:memory :pressure] (pressure (get s "memory.pressure"))))))

(defn- error-sample
  [data ^Exception e]
  (assoc data
         :status :error
         :error (.getMessage e)
         :error-class (.getName (class e))))

(defn- sample-container
  [test namespace pod-name pod-uid container]
  (try
    {:pod pod-name
     :pod-uid pod-uid
     :container container
     :status :sampled
     :cgroup (-> (k8s/kubectl! test
                               :exec pod-name
                               :-n namespace
                               :-c container
                               :--request-timeout "5s"
                               :-- :sh :-c cgroup-script)
                 parse-cgroup-output)}
    (catch Exception e
      (error-sample {:pod pod-name
                     :pod-uid pod-uid
                     :container container}
                    e))))

(defn- sample-pod
  [test namespace configured-containers pod-name]
  (try
    (let [pod        (k8s/kubectl-json! test :get :pod pod-name :-n namespace)
          pod-uid    (get-in pod [:metadata :uid])
          containers (or (seq configured-containers)
                         (some-> pod :spec :containers first :name vector))]
      (if (seq containers)
        (mapv #(sample-container test namespace pod-name pod-uid %) containers)
        [(error-sample {:pod pod-name}
                       (ex-info "pod has no application container"
                                {:pod pod-name}))]))
    (catch Exception e
      [(error-sample {:pod pod-name} e)])))

(defrecord StressProbe []
  probe/Probe
  (sample! [_ test {:keys [targets container-names]}]
    {:containers
     (->> targets
          (mapcat #(sample-pod test
                               (k8s/namespace test)
                               container-names
                               %))
          vec)}))

(defn probe
  []
  (->StressProbe))
