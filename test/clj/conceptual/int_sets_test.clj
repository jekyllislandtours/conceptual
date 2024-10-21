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
    (expect [2 6 7 8 9 10 3] (vec (i/union (int-array [2 10 3]) (int-array [6 7 8 9])))))


  (testing "issue 83"
    ;; Bug https://github.com/jekyllislandtours/conceptual/issues/83
    (let [ans (apply i/union [nil])]
      (expect some? ans)
      (expect empty? ans))))

(deftest intersection-test
  (expect some? (i/intersection (int-array []) (int-array [])))
  (expect [1] (vec (i/intersection (int-array [1]) (int-array [1]))))
  (expect [3 5] (vec (i/intersection (int-array [2 3 5]) (int-array [3 4 5]))))
  (expect [] (vec (i/intersection (int-array [2 3 5]) (int-array [6 7 8 9]))))

  (testing "issue 63"
    ;; This was a bug identified in https://github.com/jekyllislandtours/conceptual/issues/63
    (expect [] (vec (i/intersection nil nil nil))))

  (testing "issue 83"
    ;; Bug https://github.com/jekyllislandtours/conceptual/issues/83
    (let [ans (apply i/intersection [nil])]
      (expect some? ans)
      (expect empty? ans)))

  (testing "expected incorrect result because of unsorted set"
    (expect [] (vec (i/intersection (int-array [2 3 5]) (int-array [6 7 8 9 3]))))))

(deftest difference-test
  (expect some? (i/difference (int-array []) (int-array [])))
  (expect [] (vec (i/difference (int-array []) (int-array []))))
  (expect [] (vec (i/difference (int-array [1]) (int-array [1]))))
  (expect [2] (vec (i/difference (int-array [2 3 5]) (int-array [3 4 5]))))
  (expect [2 3 5] (vec (i/difference (int-array [2 3 5]) (int-array [6 7 8 9]))))

  (testing "issue 83"
    ;; Bug https://github.com/jekyllislandtours/conceptual/issues/83
    (let [ans (apply i/difference [nil])]
      (expect some? ans)
      (expect empty? ans)))

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


(deftest index-of-test
  (expect 0 (i/index-of 1 (int-array [1 2])))
  (expect 1 (i/index-of 2 (int-array [1 2])))
  (expect -1 (i/index-of 3 (int-array [1 2])))
  (expect -1 (i/index-of 3 nil)))

(deftest member?-test
  (expect true (i/member? 2 (int-array [1 2])))
  (expect false (i/member? 3 (int-array [1 2])))
  (expect false (i/member? 3 nil)))

(deftest contains?-test
  (expect true (i/contains? (int-array [1 2]) 2))
  (expect false (i/contains? (int-array [1 2]) 3))
  (expect false (i/contains? nil 3)))
