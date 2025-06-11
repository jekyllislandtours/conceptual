(ns conceptual.core.bootstrap-test
  (:require
   [conceptual.core :as c]
   [expectations.clojure.test :refer [expect]]
   [clojure.test :refer [deftest]]))

(deftest create-db-test!
  (c/create-db!)
  (println @c/*db*)
  (expect 13 (c/max-id))
  (expect true (c/value :db/tag? :db/tag?))
  (expect true (c/value :db/tag? :db/property?))
  (expect 14 (count (c/value :db/ids :db/key))))
