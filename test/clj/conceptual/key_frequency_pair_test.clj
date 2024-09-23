(ns conceptual.key-frequency-pair-test
  (:require
   [clojure.test :refer [deftest testing]]
   [expectations.clojure.test :refer [expect]])
  (:import
   (conceptual.core KeyFrequencyPair)
   (java.util Arrays)))


(defn ->clj
  [^KeyFrequencyPair x]
  {:key (.getKey x) :frequency (.getFrequency x)})

(deftest sort-test
  (let [xs (into-array KeyFrequencyPair [(KeyFrequencyPair. 1 1)
                                         (KeyFrequencyPair. 3 9)
                                         (KeyFrequencyPair. 1 7)
                                         (KeyFrequencyPair. 5 5)])]
    (Arrays/sort xs KeyFrequencyPair/KeyFrequencyPairComparator)

    (expect [{:key 3, :frequency 9}
             {:key 1, :frequency 7}
             {:key 5, :frequency 5}
             {:key 1, :frequency 1}]
            (mapv ->clj xs))))
