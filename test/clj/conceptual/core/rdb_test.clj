(ns conceptual.core.rdb-test
  (:require
   [conceptual.core :as c]
   [conceptual.test.core :as test.core]
   [expectations.clojure.test :refer [expect]]
   [clojure.test :refer [deftest use-fixtures]]))


(use-fixtures :each test.core/with-rdb)

(deftest test-data-test
  (expect (c/value :db/id :hello/world) (c/value :test/parent :hello/dude))
  (expect  #{(c/value :db/id :hello/there)
             (c/value :db/id :hello/dude)}
           (into #{} (c/value :test/children :hello/world))))

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
  (let [c (c/seek :hello/world)]
    (expect {:db/key :hello/world
             :test/string "World!"} (c/->persistent-map c))))

(deftest project-map-test
  ;; keys->ids puts the keys in order
  (expect [0 1] (vec (c/keys->ids [:db/key :db/id])))
  (expect [1 0] (vec (c/normalize-ids [:db/key :db/id])))
  (expect [#:db{:id 0 :key :db/id} #:db{:id 1 :key :db/key}]
          (->> (c/ids :db/property?)
               (c/project-map [:db/id :db/key])
               (take 2)
               vec))
  (expect [#:db{:id 0 :key :db/id} #:db{:id 1 :key :db/key}]
          (->> (c/ids :db/property?)
               (c/project-map [:db/key :db/id])
               (take 2)
               vec)))


(deftest tag-ids-kept-up-to-date-test
  (let [id (c/key->id :hello/world)]
    (expect true (contains? (c/seek :hello/world) :test/tag?))
    (expect true (contains? (set (c/ids :test/tag?)) id))
    (c/with-aggr [aggr]
      (c/replace! aggr (-> (c/seek :hello/world)
                           (dissoc :test/tag?))))

    (expect false (contains? (c/seek :hello/world) :test/tag?))
    (expect false (contains? (set (c/ids :test/tag?)) id))))
