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

(deftest create-db-test!
  (c/create-db!)
  (println @c/*db*)
  (expect 13 (c/max-id))
  (expect true (c/value :db/tag? :db/tag?))
  (expect true (c/value :db/tag? :db/property?))
  (expect 14 (count (c/value :db/ids :db/key))))
