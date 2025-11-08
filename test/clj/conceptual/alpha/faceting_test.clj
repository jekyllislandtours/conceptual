(ns conceptual.alpha.faceting-test
  (:require
   [conceptual.alpha.faceting :as faceting]
   [clojure.test :refer [deftest]]
   [conceptual.test.core :as test.core]
   [conceptual.core :as c]
   [expectations.clojure.test :refer [expect use-fixtures]]))


(use-fixtures :each test.core/with-rdb)

(deftest to-many-relations-by-frequency-test
  (let [key-id (c/key->id :sf/interests)
        all-interests (->> (c/scan :sf/interests)
                           (mapcat :sf/interests)
                           sort)
        min-id (first all-interests)
        max-id (last all-interests)]
    (expect [501 1 502 3 503 2 505 1 506 1 507 3 509 1]
            (vec (faceting/to-many-relations-by-frequency key-id min-id max-id (c/ids :sf/interests))))))
