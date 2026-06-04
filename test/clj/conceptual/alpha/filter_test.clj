(ns conceptual.alpha.filter-test
  (:require
   [clojure.test :refer [deftest use-fixtures testing]]
   [conceptual.test.core :as test.core]
   [conceptual.core :as c]
   [conceptual.int-sets :as i]
   [clojure.spec.alpha :as s]
   [conceptual.alpha.filter :as f]
   [expectations.clojure.test :refer [expect]])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each test.core/with-rdb)

(defmacro expect-error
  [error-kw expr]
  `(try
     ~expr
     (catch ExceptionInfo ex#
       (expect ~error-kw (f/error-code ex#)))))

(deftest op-sexp-test
  (expect [:sexp/op-field-val
           #:filter{:op [:op/comparison '=]
                    :field 'foo/bar
                    :value [:type/number 23]}]
          (s/conform ::f/op-sexp '(= foo/bar 23)))

  (expect [:sexp/op-val-field
           #:filter{:op [:op/comparison '=], :value [:type/number 23], :field 'foo/bar}]
          (s/conform ::f/op-sexp '(= 23 foo/bar)))

  (expect [:sexp/op-field-val
           #:filter{:op [:op/set 'contains?],
                    :field 'foo/bar,
                    :value [:type/numbers-coll #{23 42}]}]
          (s/conform ::f/op-sexp '(contains? foo/bar [23 42])))

  (expect true (s/valid? ::f/op-sexp '(-> 23 foo/bar)))
  (expect false (s/valid? ::f/op-sexp '(= 23 foo/bar 45)))

  (expect false (s/valid? ::f/sexp '(and ((= foo/bar 23))))))



(deftest not-sexp-test
  (expect true (s/valid? ::f/not-sexp '(not a/tag?)))
  (expect true (s/valid? ::f/not-sexp '(not (= foo/bar 5))))
  ;; don't support double/triple etc negatives
  (expect false (s/valid? ::f/not-sexp '(not (not sf/android?))))
  (expect false (s/valid? ::f/not-sexp '(not (not (not sf/android?)))))
  ;; one and only one item to the right of the 'not
  (expect false (s/valid? ::f/not-sexp '(not)))
  (expect false (s/valid? ::f/not-sexp '(not a/tag? b/tag?)))
  (expect false (s/valid? ::f/not-sexp '(not (and a/tag? b/tag?)
                                             (or foo/tag?)))))


(deftest not-sexp-conform-test
  (expect {:op/not 'not
           :single/sexp [:sexp/field 'a/tag?]}
          (s/conform ::f/not-sexp '(not a/tag?)))

  (expect {:op/not 'not
           :single/sexp [:sexp/op [:sexp/op-field-val {:filter/field 'foo/bar
                                                       :filter/op [:op/comparison '=]
                                                       :filter/value [:type/number 5]}]]}
          (s/conform ::f/not-sexp '(not (= foo/bar 5))))

  (expect {:op/not 'not
           :single/sexp [:sexp/op [:sexp/op-val {:filter/op [:op/custom 'search]
                                                 :filter/value [:type/string "meow"]}]]}
          (s/conform ::f/not-sexp '(not (search "meow")))))


(deftest logical-sexp-test
  (expect true (s/valid? ::f/logical-sexp '(and a/tag?)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? b/tag?)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? foo/bar)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? foo/bar (exists? hello/bye))))
  (expect true (s/valid? ::f/logical-sexp '(or b/tag? (not a/tag?))))
  (expect true (s/valid? ::f/logical-sexp '(or b/tag? (not a/tag?))))
  (expect false (s/valid? ::f/logical-sexp '(meow))))


(deftest logical-sexp-conform-test
  (expect {:op/boolean 'and
           :list/sexp [[:sexp/field 'a/tag?]]}
          (s/conform ::f/logical-sexp '(and a/tag?)))

  (expect {:op/boolean 'and
           :list/sexp [[:sexp/field 'a/tag?]
                       [:sexp/field 'b/tag?]]}
          (s/conform ::f/logical-sexp '(and a/tag? b/tag?)))

  (expect {:op/boolean 'and
           :list/sexp [[:sexp/field 'a/tag?]
                       [:sexp/field 'b/tag?]
                       [:sexp/op [:sexp/op-field {:filter/op [:op/set 'exists?]
                                                  :filter/field 'hello/bye}]]]}
          (s/conform ::f/logical-sexp '(and a/tag? b/tag? (exists? hello/bye)))))

(deftest normalize-test
  (expect '(and (= foo/bar 23)) (f/normalize '(= foo/bar 23)))
  (expect '(and (= foo/bar 23)) (f/normalize '(and (= foo/bar 23))))
  (expect '(or (= foo/bar 23)) (f/normalize '(or (= foo/bar 23))))
  (expect '(not (= foo/bar 23)) (f/normalize '(not (= foo/bar 23))))
  (expect '(and foo/bar) (f/normalize 'foo/bar))
  ;; not semantically correct but the output is acceptable
  (expect '(and ((= foo/bar 23))) (f/normalize '((= foo/bar 23)))))

(deftest sexp-test
  (expect [:sexp/op
           [:sexp/op-field-val
            #:filter{:op [:op/set 'contains?]
                     :field 'life/answer
                     :value [:type/numbers-coll #{99 42}]}]]
          (s/conform ::f/sexp '(contains? life/answer [42 99])))

  (expect [:sexp/logical
           {:op/boolean 'and
            :list/sexp
            [[:sexp/logical
              {:op/boolean 'or
               :list/sexp
               [[:sexp/op
                 [:sexp/op-val-field
                  #:filter{:op [:op/comparison '=]
                           :value [:type/number 23],
                           :field 'foo/bar}]]
                [:sexp/op
                 [:sexp/op-val-field
                  #:filter{:op [:op/comparison '=]
                           :value [:type/boolean true]
                           :field 'some/tag?}]]]}]
             [:sexp/op
              [:sexp/op-field-val
               #:filter{:op [:op/comparison '>]
                        :field 'life/answer
                        :value [:type/number 42]}]]]}]
          (s/conform ::f/sexp '(and (or (= 23 foo/bar)
                                        (= true some/tag?))
                                    (> life/answer 42)))))



(defn test-db-ids
  []
  (c/ids :test/id))

(defn eval-sexp
  ([sexp] (eval-sexp sexp (test-db-ids)))
  ([sexp init-ids]
   (eval-sexp {} (f/get-registry) sexp init-ids))
  ([ctx registry sexp]
   (eval-sexp ctx registry sexp (test-db-ids)))
  ([ctx registry sexp init-ids]
   (->> (f/evaluate ctx registry sexp init-ids)
        (map (partial c/value :test/id))
        set)))


(deftest simple-evaluate-numbers-test
  (binding [f/*enable-index-scan* true]
    (testing "= op field value"
      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(= test/int 3456))))
    (testing "= op value field"
      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(= 3456 test/int))))


    (testing "> op field value"
      (expect #{:hello/friend :hello/dude :hello/there}
              (eval-sexp '(> test/int 2000))))

    (testing "> op value field"
      (expect #{:hello/world}
              (eval-sexp '(> 2000 test/int))))

    (testing ">= op field value"
      (expect #{:hello/friend :hello/there :hello/dude}
              (eval-sexp '(>= test/int 2345))))

    (testing ">= op value field"
      (expect #{:hello/world :hello/there}
              (eval-sexp '(>= 2345 test/int))))))


(deftest simple-evaluate-strings-test
  (binding [f/*enable-index-scan* true]
    (testing "= op field value"
      (expect #{:hello/dude}
              (eval-sexp '(= test/string "Dude"))))

    (testing "= op value field"
      (expect #{:hello/dude}
              (eval-sexp '(= "Dude" test/string))))

    (testing "> throws"
      (expect-error ::f/unsupported-operator
                    (eval-sexp '(> "Dude" test/string))))

    (testing ">= throws"
      (expect-error ::f/unsupported-operator
              (eval-sexp '(>= "Dude" test/string))))

    (testing "< throws"
      (expect-error ::f/unsupported-operator
              (eval-sexp '(< "Dude" test/string))))

    (testing "<= throws"
      (expect-error ::f/unsupported-operator
              (eval-sexp '(<= "Dude" test/string))))))


(deftest simple-tag-test
  ;; NB doesn't require f/*enable-index-scan* binding to be set
  (testing "true op field value"
    (expect #{:hello/world :hello/there :hello/dude}
            (eval-sexp '(= test/tag? true))))

  (testing "true op value field"
    (expect #{:hello/world :hello/there :hello/dude}
            (eval-sexp '(= true test/tag?))))

  (testing "false op field value"
    (expect #{:hello/friend}
            (eval-sexp '(= test/tag? false))))

  (testing "false op value field"
    (expect #{:hello/friend}
            (eval-sexp '(= false test/tag?)))))


(deftest not=-test
  (c/with-aggr [aggr]
    (c/insert! aggr {:test/id :bye/bye
                     :test/long 999999999999}))

  ;; bye/bye appears because init-ids is set of concept ids having :test/id
  ;; which :bye/bye is part of and we
  (testing "not= tag true"
    (expect #{:hello/friend :bye/bye}
            (eval-sexp '(not= test/tag? true))))

  (testing "not= tag false"
    (expect #{:hello/world :hello/there :hello/dude}
            (eval-sexp '(not= test/tag? false))))

  (binding [f/*enable-index-scan* true]
    (testing "not= int"
      (expect #{:hello/world :hello/there}
              (eval-sexp '(not= test/int 3456))))

    (testing "not= string"
      (expect #{:hello/friend :hello/world :hello/there}
              (eval-sexp '(not= test/string "Dude"))))


    (testing "not= on field that may not exist"
      ;; note :bye/bye not in result since test/int not present
      (expect #{:hello/world :hello/there}
              (eval-sexp '(not= test/int 3456))))))

(deftest contains?-test
  (binding [f/*enable-index-scan* true]
    (testing "contains? 1 int"
      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(contains? [3456] test/int))))

    (testing "contains? multiple ints"
      (expect #{:hello/friend :hello/dude :hello/world}
              (eval-sexp '(contains? [3456 1234] test/int))))

    (testing "contains? 1 string"
      (expect #{:hello/world}
              (eval-sexp '(contains? ["World"] test/string))))

    (testing "contains? multiple strings"
      (expect #{:hello/dude :hello/world}
              (eval-sexp '(contains? ["World" "Dude"] test/string))))

    (testing "wrong order"
      (expect-error ::f/scalar-value-required
              (eval-sexp '(contains? test/int [3456]))))))

(deftest contains?-sym-is-coll-test
  (binding [f/*enable-index-scan* true]
    (expect #{:hello/dude}
            (eval-sexp '(contains? test/collection 300)))

    (testing "val should be a scalar"
      (expect-error ::f/scalar-value-required
                    (eval-sexp '(contains? test/collection [300]))))

    (testing "collection required as first arg to contains?"
      (expect-error ::f/collection-required
                    (eval-sexp '(contains? 300 test/collection))))))


(deftest intersects?-test
  (binding [f/*enable-index-scan* true]
    (testing "intersects? no results"
      (expect #{}
              (eval-sexp '(intersects? [999999 8888] test/collection))))

    (testing "intersects? 1 int"
      (expect #{:hello/world}
              (eval-sexp '(intersects? [100] test/collection))))

    (testing "intersects? multiple"
      (expect #{:hello/there :hello/world}
              (eval-sexp '(intersects? [100 200] test/collection))))


    (testing "intersects? multiple string"
      (expect #{:hello/friend}
              (eval-sexp '(intersects? ["abc" "def"] test/collection))))

    (testing "intersects? order indifferent"
      (expect true
              (= #{:hello/there :hello/dude}
                 (eval-sexp '(intersects? [201 300] test/collection))
                 (eval-sexp '(intersects? test/collection [201 300])))))))


(deftest subset?-test
  (binding [f/*enable-index-scan* true]
    (testing "subset? no results"
      (expect #{}
              (eval-sexp '(subset? [999999 8888] test/collection))))

    (testing "subset? 1 int"
      (expect #{:hello/world}
              (eval-sexp '(subset? [100] test/collection))))

    (testing "subset? multiple no results"
      (expect #{}
              (eval-sexp '(subset? [100 200] test/collection))))


    (testing "subset? multiple"
      (expect #{:hello/there :hello/dude}
              (eval-sexp '(subset? [201 202] test/collection))))))


(deftest superset?-test
  (binding [f/*enable-index-scan* true]
    (testing "superset? no results"
      (expect #{}
              (eval-sexp '(superset? [999999 8888] test/collection))))

    (expect #{:hello/dude}
            (eval-sexp '(superset? [300 201 202] test/collection)))

    (expect #{:hello/there :hello/dude}
            (eval-sexp '(superset? [300 201 202 200] test/collection)))

    (expect #{:hello/there :hello/dude}
            (eval-sexp '(superset? test/collection [201 202])))))

(deftest exists?-test
  (testing "exists? no results"
    (expect #{}
            (eval-sexp '(exists? this-is/not-a-field)))
    (expect #{:hello/world :hello/dude}
            (eval-sexp '(exists? test/external-id)))))

(deftest exists-and-field-predicate-equivalent?-test
  (testing "exists? no results"
    (expect true
            (= #{}
               (eval-sexp '(exists? this-is/not-a-field))
               (eval-sexp '(and this-is/not-a-field))
               (eval-sexp 'this-is/not-a-field)))
    (expect true
            (= #{:hello/world :hello/dude}
               (eval-sexp '(exists? test/external-id))
               (eval-sexp '(and test/external-id))
               (eval-sexp 'test/external-id)))))

(deftest and-test
  (binding [f/*enable-index-scan* true]
    (testing "degenerate case"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(and (= test/tag? true)))))

    (testing "tag with comparison"
      (expect #{:hello/dude :hello/there}
              (eval-sexp '(and (= test/tag? true)
                               (> test/int 2000)))))

    (testing "tag with comparison"
      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (> test/int 2000)
                               (= test/string "Dude")))))

    (testing "exists"
      (expect #{:hello/dude}
              (eval-sexp '(and (exists? test/external-id)
                               (= test/string "Dude")))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/dude}
                 (eval-sexp '(and (= test/int 3456)
                                  (= test/string "Dude")))
                 (eval-sexp '(and (= test/string "Dude")
                                  (= test/int 3456))))))

    (testing "oddly structured"
      (expect #{:hello/dude :hello/there}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)))))

      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)
                                    (and (= test/string "Dude"))))))

      (expect #{:hello/dude}
              (eval-sexp '(and (= test/tag? true)
                               (and (> test/int 2000)
                                    (= test/string "Dude"))))))


    (testing "fields as predicates"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(and test/tag?)))

      (expect #{:hello/dude :hello/there}
              (eval-sexp '(and test/tag?
                               (and (> test/int 2000)))))

      (expect #{:hello/world :hello/dude}
              (eval-sexp '(and test/external-id)))

      (expect #{:hello/dude}
              (eval-sexp '(and test/external-id test/tag? test/nice?))))))


(deftest or-test
  (binding [f/*enable-index-scan* true]
    (testing "degenerate case"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(or (= test/tag? true)))))

    (testing "tag with comparison"
      (expect #{:hello/dude :hello/there :hello/world :hello/friend}
              (eval-sexp '(or (= test/tag? true)
                              (= test/int 3456)))))

    (testing "> and ="
      (expect #{:hello/friend :hello/world}
              (eval-sexp '(or (= test/string "World")
                              (= test/string "Friend")))))

    (testing "exists"
      (expect #{:hello/dude :hello/world :hello/there}
              (eval-sexp '(or (exists? test/external-id)
                              (= test/string "There")))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/friend :hello/world}
                 (eval-sexp '(or (= test/string "World")
                                 (= test/string "Friend")))
                 (eval-sexp '(or (= test/string "Friend")
                                 (= test/string "World"))))))

    (testing "oddly structured"
      (expect #{:hello/friend :hello/world}
              (eval-sexp '(or (= test/string "World")
                              (or (= test/string "Friend"))))))


    (testing "fields as predicates"
      (expect #{:hello/dude :hello/there :hello/world}
              (eval-sexp '(or test/tag?)))

      (expect #{:hello/dude :hello/world}
              (eval-sexp '(or test/external-id)))

      (expect #{:hello/dude :hello/world}
              (eval-sexp '(or test/external-id test/nice?))))))


(deftest and-or-test
  (binding [f/*enable-index-scan* true]
    (testing "base"
      (expect #{:hello/friend}
              (eval-sexp '(and (= test/int 3456)
                               (or (= test/string "World")
                                   (= test/string "Friend"))))))

    (testing "order indifferent"
      (expect true
              (= #{:hello/friend}
                 (eval-sexp '(and (= test/int 3456)
                                  (or (= test/string "World")
                                      (= test/string "Friend"))))
                 (eval-sexp '(and (or (= test/string "World")
                                      (= test/string "Friend"))
                                  (= test/int 3456))))))

    (testing "fields as predicates"
      (expect #{}
              (eval-sexp '(and (= test/int 3456)
                               test/nice?
                               (or (= test/string "World")
                                   (= test/string "Friend")))))

      (expect #{:hello/friend :hello/dude}
              (eval-sexp '(and (= test/int 3456)
                               (or test/nice?
                                   (= test/string "World")
                                   (= test/string "Friend")))))

      (expect #{:hello/dude}
              (eval-sexp '(and (= test/int 3456)
                               test/tag?
                               (or test/nice?
                                   (= test/string "World")
                                   (= test/string "Friend"))))))))


(deftest or-and-test
  (binding [f/*enable-index-scan* true]
    (testing "base"
      (expect #{:hello/friend :hello/dude :hello/there}
              (eval-sexp '(or (= test/int 3456) ;; friend and dude
                               (and (= test/boolean false) ;; there
                                    (= test/string "There"))))))))




(deftest custom-reducer-test
  (let [tuple-counter (volatile! 0)
        field-counter (volatile! 0)
        context-counter (volatile! 0)
        tuple-custom-fn (fn [_ctx _filter-info ids]
                          (vswap! tuple-counter inc)
                          ids)
        field-custom-fn (fn [_ctx _filter-info ids]
                          (vswap! field-counter inc)
                          ids)
        context-custom-fn (fn [{:keys [custom?] :as _ctx} _filter-info ids]
                            (when custom?
                              (vswap! context-counter inc)
                              ids))
        registry (-> (f/new-registry)
                     (f/register-reducer! '[= test/int] tuple-custom-fn)
                     (f/register-reducer! 'test/string field-custom-fn)
                     (f/register-reducer! 'test/keyword context-custom-fn))]

    (expect #{:hello/there :hello/world :hello/friend :hello/dude}
            (eval-sexp {} registry '(= test/int 3456)))
    (expect 1 @tuple-counter)

    (expect #{:hello/there :hello/world :hello/friend :hello/dude}
            (eval-sexp {} registry '(= test/string "non-existing-value")))
    (expect 1 @field-counter)

    (testing "reducer with context"
      (binding [f/*enable-index-scan* true]
        ;; as expected no matching value
        (expect #{} (eval-sexp {:custom? false} registry '(= test/keyword :non-existent-ii3j8ejafajeija)))

        (expect 0 @context-counter)

        ;; reducer invoked that returns all ids passed in
        (expect #{:hello/there :hello/world :hello/friend :hello/dude}
                (eval-sexp {:custom? true} registry '(= test/keyword :non-existent-ii3j8ejafajeija)))
        (expect 1 @context-counter)))))


(deftest custom-op-val-form-test
  (let [search-counter (volatile! 0)
        search-op-fn (fn [_ctx _filter-info ids]
                       (vswap! search-counter inc)
                       ids)
        registry (-> (f/new-registry)
                     (f/register-op! 'search search-op-fn))]
    (expect true (s/valid? ::f/sexp '(search "hello")))
    (f/evaluate registry '(search "hello") i/+empty+)
    (expect 1 @search-counter)))

(deftest custom-op-form-test
  (let [full-moon-counter (volatile! 0)
        full-moon-op-fn (fn [_ctx _filter-info ids]
                          (vswap! full-moon-counter inc)
                          ids)
        registry (-> (f/new-registry)
                     (f/register-op! 'full-moon? full-moon-op-fn))]

    (expect true (s/valid? ::f/sexp '(full-moon?)))
    (f/evaluate registry '(full-moon?) i/+empty+)
    (expect 1 @full-moon-counter)))

(deftest namespaced-custom-op-form-test
  (let [full-moon-counter (volatile! 0)
        full-moon-op-fn (fn [_ctx _filter-info ids]
                          (vswap! full-moon-counter inc)
                          ids)
        registry (-> (f/new-registry)
                     (f/register-op! 'astro/full-moon? full-moon-op-fn))]
    (expect true (s/valid? ::f/sexp '(astro/full-moon?)))
    (f/evaluate registry '(astro/full-moon?) i/+empty+)
    (expect 1 @full-moon-counter)))


(deftest unknown-field-as-predicate-test
  (testing "returns empty, does not throw error"
    (expect #{} (eval-sexp '(or madeup/field) i/+empty+))
    (expect #{} (eval-sexp '(and madeup/field)))))


(deftest exists-tag-test
  ;; https://github.com/jekyllislandtours/conceptual/issues/95
  (expect #{:hello/there :hello/world :hello/dude} (eval-sexp '(exists? test/tag?))))


(defn eval-sf-sexp
  ([sexp] (eval-sf-sexp sexp (c/ids :sf/id)))
  ([sexp init-ids]
   (eval-sf-sexp {} sexp init-ids))
  ([ctx sexp init-ids]
   (->> (f/evaluate ctx sexp init-ids)
        (map (partial c/value :sf/id))
        set)))

(deftest top-level-tag-test
  (expect #{"data"} (eval-sf-sexp 'sf/android?))
  (expect #{"troi"} (eval-sf-sexp 'sf/betazoid?))
  (expect #{"troi"} (eval-sf-sexp '(exists? sf/betazoid?)))
  (expect #{"data"} (eval-sf-sexp '(exists? sf/android?)))
  (expect #{"data" "troi"} (eval-sf-sexp '(or (exists? sf/betazoid?) (exists? sf/android?))))
  (expect #{} (eval-sf-sexp '(and sf/betazoid? sf/android?)))
  (expect #{"data" "troi"} (eval-sf-sexp '(or sf/betazoid? sf/android?))))


(deftest field-predicate-test
  ;; This tests that any field defined in the schema automatically works
  ;; as if there is an implicit `exists?` predicate
  (expect #{"bev-crusher" "yar"} (eval-sf-sexp 'sf/position))
  (expect #{"uss-e" "uss-d"} (eval-sf-sexp 'sf/team-ids))
  (expect #{"uss-e" "uss-d" "bev-crusher" "yar"} (eval-sf-sexp '(or sf/position sf/team-ids)))
  (expect #{} (eval-sf-sexp '(and sf/position sf/team-ids))))


(deftest not-test
  (let [non-android-ids (->> (c/ids :sf/android?)
                             (i/difference (c/ids :sf/id))
                             (c/mapv :sf/id)
                             set)]

    (expect false (contains? non-android-ids "data"))

    (testing "not with a field/tag"
      (expect non-android-ids (eval-sf-sexp '(not sf/android?))))

    (testing "not not is unsupported syntax"
      (expect-error ::f/invalid-syntax (eval-sf-sexp '(not (not sf/android?)))))

    (binding [f/*enable-index-scan* true]
      (expect non-android-ids (eval-sf-sexp '(not (contains? ["data"] sf/id))))
      (expect non-android-ids (eval-sf-sexp '(not (= "data" sf/id)))))))
