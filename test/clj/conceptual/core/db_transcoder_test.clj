(ns conceptual.core.db-transcoder-test
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Keyword PersistentHashMap]
           [java.util Date]
           [java.time Instant]
           [conceptual.core DBTranscoder RDB]))

(defn declare-test-schema! []
  (s/declare-properties!
   [[:test/string String]
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
    [:test/edn PersistentHashMap]])
  (s/declare-to-one-relations! [[:test/parent]])
  (s/declare-to-many-relations! [[:test/children]])
  (s/declare-tags! [[:test/tag?]]))

(def +test-data+
  [{:db/key :hello/world
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
    :test/tag? true}
   {:db/key :hello/there
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
    :test/tag? true}
   {:db/key :hello/dude
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
    :test/tag? true}])

(deftest basic-pickle-test
  (testing "The most basic transcoder test"
    (let [pickle-path "temp/test_pickle.sz"]
      ;; ensure pickle path
      (io/make-parents pickle-path)

      ;; pickle setup
      (c/reset-db!)
      (is (= 12 (c/max-id)))

      ;; compact the PersistentDB into an RDB type
      (c/compact!)
      (is (= 12 (c/max-id)))

      ;; type should be RDB
      (is (instance? conceptual.core.RDB (c/db)))

      ;; pickle round-trip
      (c/pickle! pickle-path)
      (c/load-pickle! :filename pickle-path)
      (is (= 12 (c/max-id)))

      (io/delete-file pickle-path))))

(deftest pickle-type-test
  (testing "DBTranscoder type test"
    (let [pickle-path "temp/test_pickle.sz"]
      ;; ensure pickle path
      (io/make-parents pickle-path)

      ;; pickle setup
      (c/create-db!)
      (c/prime-db!)
      (is (= 12 (c/max-id)))

      ;; declare a test schema
      (declare-test-schema!)

      ;; insert test data
      (c/with-aggr [aggr]
        (doseq [data +test-data+]
          (c/insert! aggr data)))

      ;; compact the PersistentDB into an RDB type
      (c/compact!)

      ;; pickle round-trip
      (c/pickle! pickle-path)

      (c/load-pickle! :filename pickle-path)

      (doseq [data +test-data+]
        (is (= (-> data (dissoc :test/date :test/instant :test/class))
               (-> (c/seek (:db/key data))
                   c/->persistent-map
                   (dissoc :db/id :test/date :test/instant :test/class))))
        (is (= (-> data :test/instant (.toEpochMilli))
               (-> (c/value :test/instant (:db/key data))
                   (.toEpochMilli))))
        ;; dates become instants... do we like?
        (is (= (-> data :test/date (.getTime))
               (-> (c/value :test/date (:db/key data))
                   (.toEpochMilli))))
        (is (= (-> data :test/class (.getName))
               (-> (c/value :test/class (:db/key data))
                   (.getName)))))

      ;; cleanup
      (io/delete-file pickle-path))))
