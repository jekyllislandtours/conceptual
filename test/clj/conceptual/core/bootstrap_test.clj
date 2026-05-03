(ns conceptual.core.bootstrap-test
  (:require
   [conceptual.core :as c]
   [expectations.clojure.test :refer [expect]]
   [clojure.test :refer [deftest]]))


(def basic-pickle-max-id 15) ;; 15 base properties ie :db/id :db/key :db/relation? etc defined by the system

(deftest create-db-test!
  (c/create-db!)
  (expect basic-pickle-max-id (c/max-id))
  (expect true (c/value :db/tag? :db/tag?))
  (expect true (c/value :db/tag? :db/property?))
  (expect (inc basic-pickle-max-id) (count (c/value :db/ids :db/key))))
