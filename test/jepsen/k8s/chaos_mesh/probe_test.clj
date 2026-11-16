(ns jepsen.k8s.chaos-mesh.probe-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.probe :as probe])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(deftest runner-samples-until-stopped-test
  (let [calls  (atom 0)
        latch  (CountDownLatch. 2)
        p      (reify probe/Probe
                 (sample! [_ _test _context]
                   (swap! calls inc)
                   (.countDown latch)
                   {:ok true}))
        runner (probe/runner p {:interval-ms 5 :stop-timeout-ms 1000})]
    (try
      (probe/start! runner {} {:fault-kind :stress})
      (is (.await latch 1 TimeUnit/SECONDS))
      (probe/stop! runner)
      (let [stopped-at @calls]
        (Thread/sleep 30)
        (is (= stopped-at @calls)))
      (finally
        (probe/stop! runner)))))

(deftest runner-isolates-sample-failures-test
  (let [latch  (CountDownLatch. 2)
        p      (reify probe/Probe
                 (sample! [_ _test _context]
                   (.countDown latch)
                   (throw (ex-info "sample failed" {}))))
        runner (probe/runner p {:interval-ms 5 :stop-timeout-ms 1000})]
    (try
      (probe/start! runner {} {:fault-kind :stress})
      (is (.await latch 1 TimeUnit/SECONDS)
          "a failed sample does not stop later samples")
      (finally
        (probe/stop! runner)))))

(deftest runner-validation-test
  (let [p (reify probe/Probe
            (sample! [_ _test _context] nil))]
    (testing "the probe must implement Probe"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"must satisfy Probe"
                            (probe/runner identity))))
    (testing "intervals and stop timeouts must be positive"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"interval-ms"
                            (probe/runner p {:interval-ms 0})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"stop-timeout-ms"
                            (probe/runner p {:stop-timeout-ms 0}))))))
