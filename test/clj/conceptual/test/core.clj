(ns conceptual.test.core)

(defn setup []
  (println "  conceptual test setup:"))

(defn teardown []
  (println "  conceptual test teardown:"))


(defn kaocha-pre-hook! [config]
  (println "Koacha pre hook")
  (setup)
  config)

(defn kaocha-post-hook! [result]
  (println "Koacha post hook")
  (teardown)
  result)
