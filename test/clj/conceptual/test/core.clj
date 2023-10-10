(ns conceptual.test.core
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Keyword PersistentHashMap PersistentVector]
           [java.util Date]
           [java.time Instant]
           [conceptual.core DBTranscoder RDB]))

(defn setup []
  (println "  conceptual test setup:"))


(defn teardown []
  (println "  conceptual test teardown:"))


(defn kaocha-pre-hook! [config]
  (println "Koacha pre hook")
  (setup)
  config)


(defn kaocha-post-hook! [result]
  (println "Koacha post hook")
  (teardown)
  result)


(defn declare-test-schema! []
  (s/declare-properties!
   [[:test/string String]
    [:test/id Keyword]
    [:test/keyword Keyword]
    [:test/int Integer]
    [:test/long Long]
    [:test/float Float]
    [:test/double Double]
    [:test/character Character]
    [:test/boolean Boolean]
    [:test/date Date]
    [:test/instant Instant]
    [:test/class Class]
    [:test/edn PersistentHashMap]
    [:test/collection PersistentVector]])
  (s/declare-to-one-relations! [[:test/parent]])
  (s/declare-to-many-relations! [[:test/children]])
  (s/declare-tags! [[:test/tag?]]))

(def +test-data+
  [{:db/key :hello/world
    :test/id :hello/world
    :test/string "World"
    :test/keyword :hello/world
    :test/int 1234
    :test/long 12345678910
    :test/float 3.14159
    :test/double 2.71828
    :test/character \a
    :test/boolean true
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "world" 1 [2 3]}
    :test/tag? true
    :test/collection [100 101 102]}
   {:db/key :hello/there
    :test/id :hello/there
    :test/string "There"
    :test/keyword :hello/there
    :test/int 2345
    :test/long 23456789101
    :test/float 3.14159
    :test/double 2.71828
    :test/character \b
    :test/boolean false
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "there" 2 [3 4]}
    :test/tag? true
    :test/collection [200 201 202]}
   {:db/key :hello/dude
    :test/id :hello/dude
    :test/string "Dude"
    :test/keyword :hello/dude
    :test/int 3456
    :test/long 34567891012
    :test/float 3.14159
    :test/double 2.71828
    :test/character \c
    :test/boolean true
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "dude" 1 [5 6]}
    :test/tag? true
    :test/collection [300 201 202]}
   {:db/key :hello/friend
    :test/id :hello/friend
    :test/string "Friend"
    :test/keyword :hello/friend
    :test/int 3456
    :test/long 44444444444
    :test/float 3.14159
    :test/double 2.71828
    :test/character \d
    :test/boolean false
    :test/date (Date.)
    :test/instant (Instant/now)
    :test/class (class (Instant/now))
    :test/edn {:hello "friend" 2 [9 7]}
    :test/collection ["abc" "def" "ghi"]}])

(defn insert-test-data! []
  (c/with-aggr [aggr]
    (doseq [data +test-data+]
      (c/insert! aggr data)))
  (c/with-aggr [aggr]
    (c/update! {:db/key :hello/there
                :test/parent (c/value :db/id :hello/world)})
    (c/update! {:db/key :hello/dude
                :test/parent (c/value :db/id :hello/world)})
    (c/update! {:db/key :hello/world
                :test/children (int-array [(c/value :db/id :hello/there)
                                           (c/value :db/id :hello/dude)])})))

;;(c/create-db!)
;;(declare-test-schema!)

(defn with-persistentdb [f]
  (c/create-db!)
  (declare-test-schema!)
  (insert-test-data!)
  (f))

(defn with-rdb [f]
  (c/create-db!)
  (declare-test-schema!)
  (insert-test-data!)
  (c/compact!)
  (f))
