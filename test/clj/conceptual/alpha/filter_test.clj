(ns conceptual.alpha.filter-test
  (:require [clojure.test :refer [deftest use-fixtures testing]]
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

  (expect false (s/valid? ::f/op-sexp '(-> 23 foo/bar)))
  (expect false (s/valid? ::f/op-sexp '(= 23 foo/bar 45)))

  (expect false (s/valid? ::f/sexp '(and ((= foo/bar 23))))))


(deftest logical-sexp-test
  (expect true (s/valid? ::f/logical-sexp '(and a/tag?)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? b/tag?)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? foo/bar)))
  (expect true (s/valid? ::f/logical-sexp '(and a/tag? foo/bar (exists? hello/bye)))))


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
   (eval-sexp {} sexp init-ids))
  ([ctx sexp init-ids]
   (->> (f/evaluate ctx sexp init-ids)
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

(deftest not-contains?-test
  (binding [f/*enable-index-scan* true]
    (testing "not-contains? 1 int"
      (expect #{:hello/there :hello/world}
              (eval-sexp '(not-contains? [3456] test/int))))

    (testing "not-contains? multiple ints"
      (expect #{:hello/there}
              (eval-sexp '(not-contains? [3456 1234] test/int))))

    (testing "not-contains? 1 string"
      (expect #{:hello/friend :hello/there :hello/dude}
              (eval-sexp '(not-contains? ["World"] test/string))))

    (testing "not-contains? multiple strings"
      (expect #{:hello/friend :hello/there}
              (eval-sexp '(not-contains? ["World" "Dude"] test/string))))

    (testing "scalar required for 2nd arg to not-contains?"
      (expect-error ::f/scalar-value-required
              (eval-sexp '(not-contains? test/int [3456]))))))

(deftest not-contains?-sym-is-coll-test
  (binding [f/*enable-index-scan* true]
    (expect #{:hello/there :hello/world :hello/friend}
            (eval-sexp '(not-contains? test/collection 300)))

    (testing "val should be a scalar"
      (expect-error ::f/scalar-value-required
              (eval-sexp '(not-contains? test/collection [300]))))

    (testing "wrong order"
      (expect-error ::f/collection-required
              (eval-sexp '(not-contains? 300 test/collection))))))

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
              (eval-sexp '(or test/tag?) i/+empty+))

      (expect #{:hello/dude :hello/world}
              (eval-sexp '(or test/external-id) i/+empty+))

      (expect #{:hello/dude :hello/world}
              (eval-sexp '(or test/external-id test/nice?) i/+empty+)))))


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
        context-custom-fn (fn [_ctx _filter-info ids]
                            (vswap! context-counter inc)
                            ids)]
    (try
      ;; register methods that just returns ids and increments counter
      (defmethod f/custom-reducer '[= test/int] [_ _] tuple-custom-fn)
      (defmethod f/custom-reducer 'test/string [_ _] field-custom-fn)
      (defmethod f/custom-reducer 'test/keyword [{:keys [custom?]} _]
        (when custom? context-custom-fn))

      (expect #{:hello/there :hello/world :hello/friend :hello/dude}
              (eval-sexp '(= test/int 3456)))
      (expect 1 @tuple-counter)

      (expect #{:hello/there :hello/world :hello/friend :hello/dude}
              (eval-sexp '(= test/string "non-existing-value")))
      (expect 1 @field-counter)


      (testing "custom-reducer with context"
        (binding [f/*enable-index-scan* true]
          ;; as expected no matching value
          (expect #{} (eval-sexp {:custom? false} '(= test/keyword :non-existent-ii3j8ejafajeija) (test-db-ids)))

          (expect 0 @context-counter)

          ;; custom reducer invoked that returns all ids passed in
          (expect #{:hello/there :hello/world :hello/friend :hello/dude}
                  (eval-sexp {:custom? true} '(= test/keyword :non-existent-ii3j8ejafajeija) (test-db-ids)))
          (expect 1 @context-counter)))

      (finally
        (remove-method f/custom-reducer '[= test/int])
        (remove-method f/custom-reducer 'test/string)
        (remove-method f/custom-reducer 'test/keyword)))))


(deftest custom-op-val-form-test
  (let [search-counter (volatile! 0)
        search-op-fn (fn [_ctx _filter-info ids]
                       (vswap! search-counter inc)
                       ids)]

    (expect false (s/valid? ::f/sexp '(search "hello")))
    (try
      ;; register methods that just returns ids and increments counter
      (defmethod f/custom-op? 'search [_] true)
      (defmethod f/custom-op-reducer 'search [_ _] search-op-fn)

      (f/evaluate '(search "hello") (int-array []))

      (expect 1 @search-counter)

      (finally
        (remove-method f/custom-op? 'search)
        (remove-method f/custom-op-reducer 'search)))))



(deftest custom-op-form-test
  (let [full-moon-counter (volatile! 0)
        full-moon-op-fn (fn [_ctx _filter-info ids]
                          (vswap! full-moon-counter inc)
                          ids)]

    (expect false (s/valid? ::f/sexp '(full-moon?)))
    (try
      ;; register methods that just returns ids and increments counter
      (defmethod f/custom-op? 'full-moon? [_] true)
      (defmethod f/custom-op-reducer 'full-moon? [_ _] full-moon-op-fn)

      (f/evaluate '(full-moon?) (int-array []))

      (expect 1 @full-moon-counter)

      (finally
        (remove-method f/custom-op? 'full-moon?)
        (remove-method f/custom-op-reducer 'full-moon?)))))



(deftest namespaced-custom-op-form-test
  (let [full-moon-counter (volatile! 0)
        full-moon-op-fn (fn [_ctx _filter-info ids]
                          (vswap! full-moon-counter inc)
                          ids)]

    (expect false (s/valid? ::f/sexp '(astro/full-moon?)))
    (try
      ;; register methods that just returns ids and increments counter
      (defmethod f/custom-op? 'astro/full-moon? [_] true)
      (defmethod f/custom-op-reducer 'astro/full-moon? [_ _] full-moon-op-fn)

      (f/evaluate '(astro/full-moon?) (int-array []))

      (expect 1 @full-moon-counter)

      (finally
        (remove-method f/custom-op? 'astro/full-moon?)
        (remove-method f/custom-op-reducer 'astro/full-moon?)))))


(deftest unknown-field-as-predicate-test
  (testing "returns empty, does not throw error"
    (expect #{} (eval-sexp '(or madeup/field) i/+empty+))
    (expect #{} (eval-sexp '(and madeup/field)))))
