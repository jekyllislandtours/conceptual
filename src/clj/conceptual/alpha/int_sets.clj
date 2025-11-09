(ns conceptual.alpha.int-sets
  (:refer-clojure :exclude [= contains? concat conj disj keep mapcat set take])
  (:import
   (conceptual.alpha IntegerSetsAlpha)
   (conceptual.util IntArrayList)
   (java.util Arrays)))

(set! *warn-on-reflection* true)

(def +empty+ IntegerSetsAlpha/EMPTY)

(defn union
  "Returns the set union of input sorted int sets. This is `nil` safe, but NOT thread safe.
  Currently works correctly even if inputs are not sorted or have duplicates but it
  is an implementation detail that should NOT be relied on."
  (^int/1 [] +empty+)
  (^int/1 [a] (or a +empty+))
  (^int/1 [a b]
   (IntegerSetsAlpha/union (into-array int/1 [a b])))
  (^int/1 [a b c]
   (IntegerSetsAlpha/union (into-array int/1 [a b c])))
  (^int/1 [a b c d]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d])))
  (^int/1 [a b c d e]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e])))
  (^int/1 [a b c d e f]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f])))
  (^int/1 [a b c d e f g]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f g])))
  (^int/1 [a b c d e f g h]
   (IntegerSetsAlpha/union (into-array int/1 [a b c d e f g h])))
  (^int/1 [a b c d e f g h & more]
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
  "Returns the set difference. This is `nil` safe but NOT thread safe."
  (^int/1 [] +empty+)
  (^int/1 [a] (or a +empty+))
  (^int/1 [a b]
   (IntegerSetsAlpha/difference (into-array int/1 [a b])))
  (^int/1 [a b c]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c])))
  (^int/1 [a b c d]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d])))
  (^int/1 [a b c d e]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e])))
  (^int/1 [a b c d e f]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f])))
  (^int/1 [a b c d e f g]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f g])))
  (^int/1 [a b c d e f g h]
   (IntegerSetsAlpha/difference (into-array int/1 [a b c d e f g h])))
  (^int/1 [a b c d e f g h & more]
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
  "Returns the set intersection. This is `nil` safe but NOT thread safe."
  (^int/1 [] +empty+)
  (^int/1 [a] (or a +empty+))
  (^int/1 [a b]
   (IntegerSetsAlpha/intersection a b))
  (^int/1 [a b c]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c])))
  (^int/1 [a b c d]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d])))
  (^int/1 [a b c d e]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e])))
  (^int/1 [a b c d e f]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f])))
  (^int/1 [a b c d e f g]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f g])))
  (^int/1 [a b c d e f g h]
   (IntegerSetsAlpha/intersection (into-array int/1 [a b c d e f g h])))
  (^int/1 [a b c d e f g h & more]
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
  (^int/1 [] +empty+)
  (^int/1 [xs]
   (if (instance? int/1 xs)
     xs
     (IntArrayList/sortedIntSet xs))))

(defn sort!
  "Sorts an int array in place and returns the sorted array. NB. The input `xs` is
  modified in place and returned. This is not persistent and is meant to be a
  performance optimization."
  ^int/1 [^int/1 xs]
  (Arrays/sort xs) ;; this sorts in place
  xs)

(defn dedupe!
  "Dedupes a sorted int array. Returns the same array if there were no dupes which is
  not persistent and is meant to be a performance optimization."
  ^int/1 [xs]
  (IntArrayList/dedupe xs))

(defn conj
  ^int/1 [i-set x]
  (union i-set (int-array [x])))

(defn disj
  ^int/1 [i-set x]
  (difference i-set (int-array [x])))

(defn index-of
  "Returns the index of a key in a sorted int array, -1 if not found.
  Implemented via `java.util.Arrays/binarySearch`."
  [i-set x]
  (if (and i-set x)
    (max -1 (Arrays/binarySearch ^ints i-set ^int x))
    -1))

(defn contains?
  "Returns `true` if `x` is in `i-set`"
  [i-set x]
  (not= -1 (index-of i-set x)))

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
  "Returns an int set of applying `f` to each of `xs`. `f` must return
  either `nil` or an integer. `nil` return values are ignored
  and won't be in the output int set."
  ^int/1 [f xs]
  (if (instance? int/1 xs)
    (keep-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l x]
                (.add l ^Number (f x))
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
  "Returns a sorted int set of applying `f` to each of `xs`. `f` must return
  either `nil` or an integer array."
  ^int/1 [f xs]
  (if (instance? int/1 xs)
    (mapcat-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l x]
                (.addAll l ^int/1 (f x))
                l)
              ans
              xs)
      (.toSortedIntSet ans))))

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


(defn take
  "[ALPHA] Returns an int set with at most `n` unique ints from `xs`
  as they are encountered."
  ^int/1 [n xs]
  (loop [ans (java.util.HashSet/newHashSet n)
         [x & more] xs]
    (cond
      (or (clojure.core/= n (.size ans))
          (and (nil? x) (nil? more)))
      (sort! (int-array (seq ans)))

      (nil? x)
      (recur ans more)

      :else
      (do (.add ans x)
          (recur ans more)))))
