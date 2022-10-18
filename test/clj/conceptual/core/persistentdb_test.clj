(ns conceptual.core.persistentdb-test
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


(use-fixtures :once core-test/with-persistentdb)

;;(c/create-db!)

(deftest insert-test
  (expect nil (c/key->id :whats/up))
  (c/with-aggr [aggr]
    (c/insert! aggr {:db/key :whats/up}))
  (expect :whats/up (c/value :db/key :whats/up)))

(deftest update-test
  ;; add an unused key... check to see if this db/id is in that keys db/ids
  (let [prev-db-id (c/key->id :hello/world)]
    (expect int? prev-db-id)
    (expect prev-db-id (c/value :db/id :hello/world))
    (expect "World" (c/value :test/string :hello/world))
    (c/with-aggr [aggr]
      (c/update! aggr {:db/key :hello/world
                       :test/string "World!"}))
    (expect "World!" (c/value :test/string :hello/world))
    (expect prev-db-id (c/value :db/id :hello/world))))

(deftest replace-test
  (c/with-aggr [aggr]
    (c/replace! aggr {:db/key :hello/world
                      :test/string "World!"}))
  (expect {:db/key :hello/world
           :test/string "World!"} (c/->persistent-map (c/seek :hello/world))))

(deftest test-data-test
  (expect (c/value :db/id :hello/world) (c/value :test/parent :hello/dude))
  (expect  #{(c/value :db/id :hello/there)
             (c/value :db/id :hello/dude)}
           (into #{} (c/value :test/children :hello/world))))

(deftest db-map-test
  )
