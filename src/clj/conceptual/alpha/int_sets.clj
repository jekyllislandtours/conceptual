(ns conceptual.alpha.int-sets
  (:refer-clojure :exclude [contains? concat conj disj keep mapcat set =])
  (:import
   (conceptual.alpha IntegerSetsAlpha)
   (conceptual.util IntArrayList)
   (java.util Arrays)))

(set! *warn-on-reflection* true)

(def +empty+ IntegerSetsAlpha/EMPTY)

(defn union
  ([] +empty+)
  ([a] (or a +empty+))
  ([a b]
   (IntegerSetsAlpha/union (into-array int/1 [a b])))
  ([a b c]
   (IntegerSetsAlpha/union (into-array int/1 [a b c])))
  ([a b c d]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d])))
  ([a b c d e]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e])))
  ([a b c d e f]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f])))
  ([a b c d e f g]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f g])))
  ([a b c d e f g h]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f g h])))
  ([a b c d e f g h & more]
   (->> more
        (cons h)
        (cons g)
        (cons f)
        (cons e)
        (cons d)
        (cons c)
        (cons b)
        (cons a)
        (into-array int/1)
        IntegerSetsAlpha/union)))


(defn difference
  ([] +empty+)
  ([a] (or a +empty+))
  ([a b]
   (IntegerSetsAlpha/difference (into-array int/1 [a b])))
  ([a b c]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c])))
  ([a b c d]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d])))
  ([a b c d e]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e])))
  ([a b c d e f]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f])))
  ([a b c d e f g]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f g])))
  ([a b c d e f g h]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f g h])))
  ([a b c d e f g h & more]
   (->> more
        (cons h)
        (cons g)
        (cons f)
        (cons e)
        (cons d)
        (cons c)
        (cons b)
        (cons a)
        (into-array int/1)
        IntegerSetsAlpha/difference)))



(defn intersection
  ([] +empty+)
  ([a] a)
  ([a b]
   (IntegerSetsAlpha/intersection a b))
  ([a b c]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c])))
  ([a b c d]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d])))
  ([a b c d e]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e])))
  ([a b c d e f]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f])))
  ([a b c d e f g]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f g])))
  ([a b c d e f g h]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f g h])))
  ([a b c d e f g h & more]
   (->> more
        (cons h)
        (cons g)
        (cons f)
        (cons e)
        (cons d)
        (cons c)
        (cons b)
        (cons a)
        (into-array int/1)
        IntegerSetsAlpha/intersection)))

(defn set
  ([] +empty+)
  ([xs]
   (if (clojure.core/= int/1 (class xs))
     xs
     (IntArrayList/sortedIntSet xs))))

(defn sort!
  "Sorts an int array in place and returns the sorted array. NB. The input `xs` is
  modified in place and returned. This is not persistent and is meant to be a
  performance optimization."
  [^int/1 xs]
  (Arrays/sort xs) ;; this sorts in place
  xs)

(defn dedupe!
  "Dedupes a sorted int array. Returns the same array if there were no dupes which is
  not persistent and is meant to be a performance optimization."
  [xs]
  (IntArrayList/dedupe xs))

(defn =
  "Returns true if set `a` and set `b` have equal contents."
  [a b]
  (IntegerSetsAlpha/equals a b))

(defn subset?
  "Is `a` subset of `b`?"
  [a b]
  (= (intersection a b) a))

(defn superset?
  "Is `a` a superset of `b`?"
  [a b]
  (= (intersection a b) b))

(defn conj
  [i-set x]
  (union i-set (int-array [x])))

(defn disj
  [i-set x]
  (difference i-set (int-array [x])))

(defn index-of
  "Returns the index of a key in a sorted int array, -1 if not found.
  Implemented via `java.util.Arrays/binarySearch`."
  [i-set x]
  (if (and i-set x)
    (Arrays/binarySearch i-set x)
    -1))

(defn contains?
  "Returns `true` if `x` is in `i-set`"
  [i-set x]
  (pos? (index-of i-set x)))

(defn member?
  "Returns `true` if `x` is in `i-set`. Same as `contains?` but args are reversed."
  [x i-set]
  (contains? i-set x))


(defn- keep-int-set
  [f ^int/1 xs]
  (let [ans (IntArrayList/new)]
    (dotimes [i (alength xs)]
      (.add ans ^Number (f (aget xs i))))
    (.toSortedIntSet ans)))

(defn keep
  "Returns an int set of applying `f` to `xs`. `f` must return
  either `nil` or an integer. `nil` return values are ignored
  and won't be in the output int set."
  [f xs]
  (if (= int/1 (class xs))
    (keep-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l id]
                (.add l ^Number (f id))
                l)
              ans
              xs)
      (.toSortedIntSet ans))))

(defn- mapcat-int-set
  [f ^int/1 xs]
  (let [ans (IntArrayList/new)]
    (dotimes [i (alength xs)]
      (.addAll ans ^int/1 (f (aget xs i))))
    (.toSortedIntSet ans)))

(defn mapcat
  "Returns a sorted int set of applying `f` to `xs`. `f` must return
  either `nil` or an integer array."
  [f xs]
  (if (clojure.core/= int/1 (class xs))
    (mapcat-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l x]
                (.addAll l ^int/1 (f x))
                l)
              ans
              xs)
      (.toSortedIntSet ans))))
