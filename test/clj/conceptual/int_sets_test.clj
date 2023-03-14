(ns conceptual.int-sets-test
  (:require [conceptual.int-sets :as i]
            [clojure.test :refer [deftest testing]]
            [expectations.clojure.test :refer [expect]]))


(deftest equals?-test
  (expect true (i/equals? (int-array []) (int-array [])))
  (expect true (i/equals? (int-array [-1 2]) (int-array [-1 2])))
  (expect false (i/equals? (int-array [2 -1]) (int-array [-1 2])))
  (expect false (i/equals? (int-array []) (int-array [1 2])))
  (expect false (i/equals? (int-array [1]) (int-array [1 2])))

  (testing "equality doesn't care about the sets being sorted"
    (expect true (i/equals? (int-array [-1 -2]) (int-array [-1 -2])))
    (expect true (i/equals? (int-array [2 1]) (int-array [2 1])))))

(deftest union-test
  (expect some? (i/union (int-array []) (int-array [])))
  (expect [] (vec (i/union (int-array []) (int-array []))))
  (expect [1] (vec (i/union (int-array []) (int-array [1]))))
  (expect [1] (vec (i/union (int-array [1]) (int-array [1]))))
  (expect [2 3 4 5] (vec (i/union (int-array [2 3 5]) (int-array [3 4 5]))))
  ;; returns sorted int set
  (expect [2 3 5 6 7 8 9 10] (vec (i/union (int-array [2 3 5 10]) (int-array [3 6 7 8 9]))))
  (testing "expected incorrect result because of unsorted set"
    (expect [2 6 7 8 9 10 3] (vec (i/union (int-array [2 10 3]) (int-array [6 7 8 9]))))))

(deftest intersection-test
  (expect some? (i/intersection (int-array []) (int-array [])))
  (expect [1] (vec (i/intersection (int-array [1]) (int-array [1]))))
  (expect [3 5] (vec (i/intersection (int-array [2 3 5]) (int-array [3 4 5]))))
  (expect [] (vec (i/intersection (int-array [2 3 5]) (int-array [6 7 8 9]))))
  (testing "expected incorrect result because of unsorted set"
    (expect [] (vec (i/intersection (int-array [2 3 5]) (int-array [6 7 8 9 3]))))))

(deftest difference-test
  (expect some? (i/difference (int-array []) (int-array [])))
  (expect [] (vec (i/difference (int-array []) (int-array []))))
  (expect [] (vec (i/difference (int-array [1]) (int-array [1]))))
  (expect [2] (vec (i/difference (int-array [2 3 5]) (int-array [3 4 5]))))
  (expect [2 3 5] (vec (i/difference (int-array [2 3 5]) (int-array [6 7 8 9]))))
  (testing "expected incorrect result because of unsorted set"
    (expect [2 3 5] (vec (i/difference (int-array [2 3 5]) (int-array [6 7 8 9 3]))))))

(deftest subset?-test
  (expect true (i/subset? (int-array []) (int-array [])))
  (expect true (i/subset? (int-array [1]) (int-array [1 2])))
  (expect false (i/subset? (int-array [1 2]) (int-array [1])))
  (expect false (i/subset? (int-array [1 2]) (int-array [1 3 5]))))

(deftest superset?-test
  (expect true (i/superset? (int-array []) (int-array [])))
  (expect true (i/superset? (int-array [1]) (int-array [])))
  (expect true (i/superset? (int-array [1]) (int-array [1])))
  (expect true (i/superset? (int-array [1 2]) (int-array [1 2])))
  (expect true (i/superset? (int-array [1 2 3]) (int-array [1 2])))
  (expect false (i/superset? (int-array []) (int-array [1])))
  (expect false (i/superset? (int-array [1 2 3]) (int-array [1 2 5])))
  (expect false (i/superset? (int-array [1 3 5]) (int-array [1 2]))))
