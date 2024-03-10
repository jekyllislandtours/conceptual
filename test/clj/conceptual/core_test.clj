(ns conceptual.core-test
  (:require [conceptual.core :as c]
            [clojure.test :refer [deftest use-fixtures]]
            [conceptual.test.core :as test.core]
            [expectations.clojure.test :refer [expect]]))

(use-fixtures :each test.core/with-rdb)

(deftest normalize-ids-test
  (expect [0] (vec (c/normalize-ids [:db/id])))
  ;; Fixes issue https://github.com/jekyllislandtours/conceptual/issues/52
  (expect [0] (vec (c/normalize-ids (seq [:db/id])))))

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
    (expect {:key :unknown/key} (try-f {:db/id 1 :unknown/key :hello}))))
