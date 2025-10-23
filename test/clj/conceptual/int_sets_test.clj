(ns conceptual.int-sets-test
  (:require
   [conceptual.int-sets :as i]
   [clojure.test :refer [deftest testing]]
   [expectations.clojure.test :refer [expect]])
  (:import (conceptual.util IntegerSets)))


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


(deftest fast-intersection-test
  (expect [] (vec (i/fast-intersection nil)))
  (expect [] (vec (i/fast-intersection nil nil)))
  (expect [] (vec (i/fast-intersection nil (i/set [5]))))
  (expect [] (vec (i/fast-intersection (i/set []) (i/set [5]))))
  (expect [] (vec (i/fast-intersection (i/set [1 3 9]) (i/set [5]))))
  (expect [] (vec (i/fast-intersection (i/set [5]) (i/set [1 3 9]))))

  (expect [9] (vec (i/fast-intersection (i/set [1 3 9]) (i/set [5 9]))))
  (expect [9] (vec (i/fast-intersection (i/set [1 9]) (i/set [9]))))
  (expect [9] (vec (i/fast-intersection (i/set [9]) (i/set [9]))))

  (expect [9] (vec (i/fast-intersection (i/set [5 9]) (i/set [1 3 9]))))
  (expect [9] (vec (i/fast-intersection (i/set [9]) (i/set [1 9]))))

  (expect [9] (vec (i/fast-intersection (i/set [2 5 9]) (i/set [1 3 9]) (i/set [9]))))
  (expect [] (vec (i/fast-intersection (i/set [2 5 9]) (i/set [1 3 9]) (i/set [99]))))


  (expect [1] (vec (i/fast-intersection (i/set [1]) (i/set [1]))))
  (expect [1 2] (vec (i/fast-intersection (i/set [1 2]) (i/set [1 2]))))
  (expect [1 2 3] (vec (i/fast-intersection (i/set [1 2 3]) (i/set [1 2 3]))))
  (expect [1 2 3 4] (vec (i/fast-intersection (i/set [1 2 3 4]) (i/set [1 2 3 4]))))
  (expect [1 2 3 4 5] (vec (i/fast-intersection (i/set [1 2 3 4 5]) (i/set [1 2 3 4 5]))))

  (expect [3 5] (vec (i/fast-intersection (i/set [2 3 5]) (i/set [3 4 5]))))
  (expect [] (vec (i/fast-intersection (i/set [2 3 5]) (i/set [6 7 8 9]))))


  (expect [] (vec (IntegerSets/fastIntersection (into-array int/1 [nil nil nil]))))
  (expect [] (vec (IntegerSets/fastIntersection nil)))
  (expect [] (vec (IntegerSets/fastIntersection (into-array int/1 [(int-array [2])
                                                                    nil
                                                                    (int-array [2 3 5])]))))

  ;; technically not valid since we expect contents to be greater than 1
  (expect [0] (vec (i/fast-intersection (i/set [0]) (i/set [0]))))

  ;; expected incorrect result because of unsorted set
  (expect [] (vec (i/fast-intersection (int-array [2 3 5]) (int-array [6 7 8 9 3])))))


(defn fast-difference
  [& xs]
  (->> xs
       (map int-array)
       into-array
       IntegerSets/fastDifference
       vec))

(deftest fast-difference-test
  (expect [] (vec (i/fast-difference nil)))
  (expect [] (vec (i/fast-difference nil nil)))
  (expect [1] (vec (i/fast-difference (i/set [1]))))
  (expect [1] (vec (i/fast-difference (i/set [1]) nil)))
  (expect [] (vec (i/fast-difference nil (i/set [1]))))
  (expect [1] (vec (i/fast-difference (i/set [1]) (i/set []))))
  (expect [1 2] (vec (i/fast-difference (i/set [1 2]))))
  (expect [] (vec (i/fast-difference (i/set [1]) (i/set [1]))))
  (expect [] (vec (i/fast-difference (i/set [1]) (i/set [1]) (i/set [1]))))
  (expect [] (vec (i/fast-difference (i/set [1 2]) (i/set [1 2]) (i/set [1 2]))))
  (expect [2 3] (vec (i/fast-difference (i/set [1 2 3]) (i/set [1]) (i/set [1]))))
  (expect [3] (vec (i/fast-difference (i/set [1 2 3]) (i/set [1]) (i/set [2]))))
  (expect [] (vec (i/fast-difference (i/set [1 2 3]) (i/set [1]) (i/set [2]) (i/set [3]))))
  (expect [] (vec (i/fast-difference (i/set [1 2 3]) (i/set []) (i/set [1 2]) (i/set [3]))))
  (expect [] (vec (i/fast-difference (i/set [1 2 3]) (i/set []) (i/set [1]) (i/set [2 3]))))
  (expect [] (vec (i/fast-difference (i/set [1 2 3]) (i/set [1 2]) (i/set [3]) (i/set [2 3]))))
  (expect [2] (vec (i/fast-difference (i/set [1 2 3]) (i/set [1 3]) (i/set [3]) (i/set [3]))))

  (expect [2] (vec (i/difference (i/set [2 3 5]) (i/set [3 4 5]))))
  (expect [2 3 5] (vec (i/difference (i/set [2 3 5]) (i/set [6 7 8 9]))))

  (expect [] (vec (IntegerSets/fastDifference nil)))
  (expect [] (vec (IntegerSets/fastDifference (into-array int/1 [nil nil]))))

  (testing "expected incorrect result because of unsorted set"
    (expect [2 3 5] (vec (i/difference (int-array [2 3 5]) (int-array [6 7 8 9 3])))))

  (testing "issue 83"
    ;; Bug https://github.com/jekyllislandtours/conceptual/issues/83
    (let [ans (apply i/fast-difference [nil])]
      (expect some? ans)
      (expect empty? ans))))

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
  (expect -1 (i/index-of 3 nil))
  (expect -1 (i/index-of nil nil))
  (expect -1 (i/index-of nil (int-array []))))

(deftest member?-test
  (expect true (i/member? 2 (int-array [1 2])))
  (expect false (i/member? 3 (int-array [1 2])))
  (expect false (i/member? 3 nil)))

(deftest contains?-test
  (expect true (i/contains? (int-array [1 2]) 2))
  (expect false (i/contains? (int-array [1 2]) 3))
  (expect false (i/contains? nil 3))
  (expect false (i/contains? nil nil))
  (expect false (i/contains? (int-array []) nil)))


(deftest set-test
  (expect (class (int-array 0)) (class (i/set [])))
  (expect (class (int-array 0)) (class (i/set nil)))
  (expect [] (vec (i/set [])))
  (expect [1 2 3 9 27] (vec (i/set [3 9 1 27 2 3])))
  (expect [1 3 9] (vec (i/set [9 1 nil 3]))))


(deftest keep-test
  (let [f {0 0
           1 10
           2 20
           3 30
           4 nil
           5 50}]
    (expect [0 10 20 30 50]
            (vec (i/keep f (int-array (keys f)))))
    (expect [0 10 20 30 50]
            (vec (i/keep f (keys f))))))

(deftest mapcat-test
  (let [f {0 (int-array [0])
           1 (int-array [10 11])
           2 (int-array [20 11])
           3 (int-array [30 33])
           4 nil
           5 (int-array [50 33 51 53])}]
    (expect [0 10 11 20 30 33 50 51 53]
            (vec (i/mapcat f (int-array (keys f)))))
    (expect [0 10 11 20 30 33 50 51 53]
            (vec (i/mapcat f (keys f))))))

(deftest sort-test
  (let [sorted (int-array [1 2 3])]
    ;; same array reference is returned
    (expect true (= sorted (i/sort sorted)))
    (expect [1 2 3] (vec sorted)))

  (let [unsorted (int-array [1 5 3 2 2])]
    (expect true (= unsorted (i/sort unsorted)))
    (expect [1 2 2 3 5] (vec unsorted))))


(deftest dedupe-test
  (let [input (int-array [])
        output (i/dedupe input)]
    ;; same array reference is returned
    (expect true (= output input))
    (expect [] (vec output)))

  (let [input (int-array [1])
        output (i/dedupe input)]
    ;; same array reference is returned
    (expect true (= output input))
    (expect [1] (vec output)))

  (let [input (int-array [1 2 3])
        output (i/dedupe input)]
    ;; same array reference is returned
    (expect true (= output input))
    (expect [1 2 3] (vec output)))

  (let [input (int-array [1 2 2 3 5])
        output (i/dedupe input)]
    ;; different array reference is returned
    (expect false (= output input))
    (expect [1 2 3 5] (vec output))))



(deftest concat-test
  (expect [] (vec (i/concat)))
  (expect [] (vec (i/concat nil)))
  (expect [] (vec (i/concat [])))
  (expect [] (vec (i/concat (i/set nil) (i/set))))
  (expect [1 2 3] (vec (i/concat (i/set [1 2]) (i/set [2 3]))))
  (expect [1 2] (vec (i/concat (i/set [1 2]) (i/set [1]))))


  ;; 1 arity
  (expect [1 2] (vec (i/concat (i/set [1 2]))))

  ;; 2 arity
  (expect [1 2] (vec (i/concat (i/set [1 2])
                               (i/set [1]))))

  ;; 3 arity
  (expect [1 2 3] (vec (i/concat (i/set [1 2])
                                 (i/set [1])
                                 (i/set [3]))))
  ;; 4 arity
  (expect [1 2 3 4] (vec (i/concat (i/set [1 2])
                                   (i/set [1])
                                   (i/set [3])
                                   (i/set [2 4]))))

  ;; 5 arity
  (expect [1 2 3 4 5] (vec (i/concat (i/set [1 2])
                                   (i/set [1])
                                   (i/set [3])
                                   (i/set [2 4])
                                   (i/set [5]))))


  ;; rest args
  (expect [1 2 3 4 5 6 7] (vec (i/concat (i/set [1 2])
                                         (i/set [1])
                                         (i/set [3])
                                         (i/set [2 4])
                                         (i/set [5])
                                         (i/set [6 7]))))


  (expect [1 2 3 4 5 6 7] (->> [[1] [2] [3] [4] [5] [6] [7]]
                               (map i/set)
                               (apply i/concat)
                               vec)))
