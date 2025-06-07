(ns conceptual.core-test
  (:require
   [conceptual.core :as c]
   [clojure.test :refer [deftest use-fixtures testing]]
   [conceptual.test.core :as test.core]
   [expectations.clojure.test :refer [expect]]))

(use-fixtures :each test.core/with-rdb)

(deftest keys->ids-test
  (testing "unknown keys are ignored, data is sorted and unique"
    (expect (vec (sort (map c/key->id [:sf/id :db/id :sf/name])))
            (vec (c/keys->ids [:db/id :sf/id :sf/id :undefined/field-kd83jj :sf/name :not-a-field])))))

(deftest normalize-ids-test
  (expect [0] (vec (c/normalize-ids [:db/id])))
  ;; Fixes issue https://github.com/jekyllislandtours/conceptual/issues/52
  (expect [0] (vec (c/normalize-ids (seq [:db/id]))))

  (testing "unknown keys are ignored, data is not sorted and duplicates are allowed"
    (expect (mapv c/key->id [:db/id :sf/id :sf/name :sf/id])
            (vec (c/normalize-ids [:db/id :sf/id :undefined/field-kd83jj :sf/name :not-a-field :sf/id])))))

(deftest map-kvs-test
  ;; Fixes issue https://github.com/jekyllislandtours/conceptual/issues/51
  (let [f (fn [m] (#'c/map->kvs (c/db) m))
        try-f (fn [m]
                (try
                  (f m)
                  (catch Exception ex
                    (some-> ex ex-data))))]
    (expect [[0] [1]] (mapv vec (f {:db/id 1})))
    (expect [[0 1] [1 :hello]] (mapv vec (f {:db/id 1 :db/key :hello})))
    (expect {:conceptual/error :conceptual/unknown-key-error
             :unknown-key :unknown/key}
            (try-f {:db/id 1 :unknown/key :hello}))))

(deftest ids-of-unknown-key-returns-empty-test
  (expect [] (vec (c/ids :a-key-i-made-up--kjadiak383))))
