(ns conceptual.core.bootstrap-test
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [conceptual.test.core :as core-test]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [expectations.clojure.test :refer [defexpect expect expecting more more-> more-of]]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Keyword PersistentHashMap]
           [java.util Date]
           [java.time Instant]
           [conceptual.core DBTranscoder RDB]))


;;(use-fixtures :once core-test/with-persistentdb)

;;(c/-create-db!)
;;(c/dump)

(deftest -create-db-test!
  (c/-create-db!)
  (expect 3 (c/max-id))
  (expect 0 (c/value :db/id :db/id))
  (expect 1 (c/value :db/id :db/key))
  (expect (range 3) (->> (range 3)
                         (map (partial c/value :db/key))
                         (map c/key->id))))
