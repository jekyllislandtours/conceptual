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
