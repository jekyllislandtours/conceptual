(ns conceptual.int-array-list-test
  (:require
   [clojure.test :refer [deftest testing]]
   [expectations.clojure.test :refer [expect]])
  (:import (conceptual.util IntArrayList)))


(deftest add-test
  (let [l (IntArrayList/new)]
    (.add l 1)
    (.add ^Number l nil) ; nils are ignored
    (.add l 42)
    (expect 2 (count l)))


  (testing "floating point takes intValue"
    (let [l (IntArrayList/new)]
      (.add l 3.14)
      (expect 3 (.get l 0)))))

(deftest add-all-test
  (let [l (IntArrayList/new)]
    (.addAll l (range 5))
    (.addAll l '[9 10])
    (.addAll l (into (sorted-set) #{6 7 8}))
    (expect [0 1 2 3 4 9 10 6 7 8] (vec (.toIntArray l)))))


(deftest to-sorted-int-array-test
  (let [l (IntArrayList/new)]
    (.addAll l (range 5))
    (.addAll l '[9 10])
    (.addAll l (into (sorted-set) #{2 3 6}))
    (expect [0 1 2 3 4 9 10 2 3 6] (vec (.toIntArray l)))
    (expect [0 1 2 3 4 6 9 10] (vec (.toSortedIntSet l)))))


(deftest sorted-int-test
  (expect [0 1 2 3 4 6 9 10]
          (->> [0 1 2 3 4 9 10 2 3 6]
               IntArrayList/sortedIntSet
               vec)))
