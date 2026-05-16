(ns conceptual.int-sets
  "Functions in this namespace are non-lazy by default."
  (:refer-clojure :exclude [= contains? conj disj filter map mapcat remove set take])
  (:import (conceptual.util IntegerSets IntArrayList)
           (java.util Arrays)))

(set! *warn-on-reflection* true)

(def +empty+ IntegerSets/EMPTY)

(defn int-array?
  [x]
  (clojure.core/= int/1 (class x)))

(defn- chunked-reduce
  "Acts similarly to reduce except f is passed chunk-size number of
  arguments from coll after val. For example (chunked-reduce + 0 [1 2
  3 4 5 6 7 8] 3) will pass 0, 1, 2, and 3 to + in the first
  iteration."
  [f val coll chunk-size]
  (loop [current-coll coll
         acc val]
    (let [result (apply f acc (clojure.core/take chunk-size current-coll))
          rest (drop chunk-size current-coll)]
      (if (seq current-coll)
        (recur rest
               result)
        result))))

(defn intersection
  "Returns the integer intersection of the given sorted int array sets."
  (^int/1 [] +empty+)
  (^int/1 [^ints a] (or a +empty+))
  (^int/1 [^ints a ^ints b] (IntegerSets/intersection a b))
  (^int/1 [^ints a ^ints b ^ints c] (IntegerSets/intersection a b c))
  (^int/1 [^ints a ^ints b ^ints c ^ints d] (IntegerSets/intersection a b c d))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e] (IntegerSets/intersection a b c d e))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f] (IntegerSets/intersection a b c d e f))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g] (IntegerSets/intersection a b c d e f g))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h] (IntegerSets/intersection a b c d e f g h))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h & more]
   (chunked-reduce intersection (intersection a b c d e f g h) more 7)))

(defn union
  "Returns the set union of input sorted int sets. This is `nil` safe, but NOT thread safe.
  Currently works correctly even if inputs are not sorted or have duplicates but it
  is an implementation detail that should NOT be relied on."
  (^int/1 [] +empty+)
  (^int/1 [a] (or a +empty+))
  (^int/1 [a b]
   (IntegerSets/union2 (into-array int/1 [a b])))
  (^int/1 [a b c]
   (IntegerSets/union2 (into-array int/1 [a b c])))
  (^int/1 [a b c d]
   (IntegerSets/union2 (into-array int/1 [a b c d])))
  (^int/1 [a b c d e]
   (IntegerSets/union2 (into-array int/1 [a b c d e])))
  (^int/1 [a b c d e f]
   (IntegerSets/union2 (into-array int/1 [a b c d e f])))
  (^int/1 [a b c d e f g]
   (IntegerSets/union2 (into-array int/1 [a b c d e f g])))
  (^int/1 [a b c d e f g h]
   (IntegerSets/union2 (into-array int/1 [a b c d e f g h])))
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
        IntegerSets/union2)))

(defn difference
  "Returns the integer difference of the given sorted int arrays. Everything is
  subtracted from the first sorted int array set."
  ([] +empty+)
  (^int/1 [^ints a] (or a +empty+))
  (^int/1 [^ints a ^ints b] (IntegerSets/difference a b))
  (^int/1 [^ints a ^ints b ^ints c] (difference a (union b c)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d] (difference a (union b c d)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e] (difference a (union b c d e)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f] (difference a (union b c d e f)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g] (difference a (union b c d e f g)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h] (difference a (union b c d e f g h)))
  (^int/1 [^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h & more]
   (difference a (apply union a b c d e f g h more))))

(defn difference!
  "[ALPHA] Returns the set difference. This is `nil` safe but NOT thread safe. Similar to `difference` but
  avoids unnecessary allocations in edge cases."
  (^int/1 [] +empty+)
  (^int/1 [a] (or a +empty+))
  (^int/1 [a b]
   (IntegerSets/difference2 (into-array int/1 [a b])))
  (^int/1 [a b c]
   (IntegerSets/difference2 (into-array int/1 [a b c])))
  (^int/1 [a b c d]
   (IntegerSets/difference2 (into-array int/1 [a b c d])))
  (^int/1 [a b c d e]
   (IntegerSets/difference2 (into-array int/1 [a b c d e])))
  (^int/1 [a b c d e f]
   (IntegerSets/difference2 (into-array int/1 [a b c d e f])))
  (^int/1 [a b c d e f g]
   (IntegerSets/difference2 (into-array int/1 [a b c d e f g])))
  (^int/1 [a b c d e f g h]
   (IntegerSets/difference2 (into-array int/1 [a b c d e f g h])))
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
        IntegerSets/difference2)))

(defn conj
  ^int/1 [^ints a b]
  (union a (int-array [b])))

(defn disj
  ^int/1 [^ints a b]
  (difference a (int-array [b])))

(defn index-of
  "Returns the index of a key in a sorted int array, -1 if not found."
  ^long [key ^ints coll]
  (if key
    (IntegerSets/contains coll key)
    -1))

(defn index
  "[ALPHA] Same as `index-of` with args reversed."
  ^long [^ints coll key]
  (index-of key coll))

(defn member?
  "Returns `true` if `key` is in `coll`"
  [key ^ints coll]
  (not= -1 (index-of key coll)))

(defn contains?
  "Returns `true` if `key` is in `coll`"
  [^ints coll key]
  (member? key coll))

(defn =
  "Returns true if set `a` and set `b` have equal contents."
  [a b]
  (IntegerSets/equals a b))

(defn subset?
  "Is set1 a subset of set2?"
  [^ints set1 ^ints set2]
  (= (intersection set1 set2) set1))

(defn superset?
  "Is set1 a superset of set2?"
  [^ints set1 ^ints set2]
  (= (intersection set1 set2) set2))

(defn encode
  "Encodes and integer array into a byte array."
  [^ints s]
  (IntegerSets/encode s))

(defn decode
  "Decodes an encoded integer array back into into an integer array."
  [^bytes s]
  (IntegerSets/decode s))

(defn set
  ([] +empty+)
  (^int/1 [xs]
   (if (int-array? xs)
     xs
     (IntArrayList/sortedIntSet xs))))

(defn sort!
  "[ALPHA] Sorts an int array in place and returns the sorted array. NB. The input `xs` is
  modified in place and returned. This is not persistent and is meant to be a
  performance optimization."
  ^int/1 [^int/1 xs]
  (Arrays/sort xs) ;; this sorts in place
  xs)

(defn dedupe!
  "[ALPHA] Dedupes a sorted int array. Returns the same array if there were no dupes which is
  not persistent and is meant to be a performance optimization."
  ^int/1 [xs]
  (IntArrayList/dedupe xs))

(defn- map-int-set
  [f ^int/1 xs]
  (let [ans (IntArrayList/new)]
    (dotimes [i (alength xs)]
      (.add ans ^Number (f (aget xs i))))
    (.toSortedIntSet ans)))

(defn map
  "[ALPHA] Returns an int set of applying `f` to `xs`. `f` must return
  either `nil` or an integer. `nil` return values are ignored
  and won't be in the output int set. Note that the semantics of this `map`
  implementation means that `keep` is not necessary."
  ^int/1 [f xs]
  (if (int-array? xs)
    (map-int-set f xs)
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
  "[ALPHA] Returns an int set of applying `f` to `xs`. `f` must return
  either `nil` or an integer array."
  ^int/1 [f xs]
  (if (int-array? xs)
    (mapcat-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l id]
                (.addAll l ^int/1 (f id))
                l)
              ans
              xs)
      (.toSortedIntSet ans))))

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

(defn- filter-int-set
  [pred ^int/1 xs]
  (let [n (alength xs)
        ans (IntArrayList/new n)]
    (dotimes [i n]
      (let [x (aget xs i)]
        (when (pred x)
          (.add ans x))))
    (.toIntArray ans)))

(defn filter
  "[ALPHA] Returns a subset of `xs` which pass `pred`."
  [pred xs]
  (if (int-array? xs)
    (filter-int-set pred xs)
    (let [ans (IntArrayList/new)]
      (loop [[x & more] xs]
        ;; x can't be nil if we're to add to an int-array
        (when (and x (pred x))
          (.add ans ^int x))
        (when more
          (recur more)))
      (.toIntArray ans))))

(defn remove
  "[ALPHA] Removes a subset of `xs` which pass `pred`."
  [pred xs]
  (filter (complement pred) xs))


(defn binary-search-greater
  "Searches for a key in a sorted array, and returns an index to an element
   which is greater than or equal key. Returns end if nothing greater or equal
   was found, else an index satisfying the search criteria."
  ([key ^ints coll]
   (IntegerSets/binarySearchGreater coll key))
  ([key begin end ^ints coll]
   (IntegerSets/binarySearchGreater coll key begin end)))

(defn binary-search-smaller
  "Searches for a key in a sorted array, and returns an index to an element
      which is smaller than or equal key."
  ([key ^ints coll] (IntegerSets/binarySearchSmaller coll key))
  ([key begin end ^ints coll]
     (IntegerSets/binarySearchSmaller coll key begin end)))

(defn binary-search
  "Searches for a key in a sorted array."
  ([key ^ints coll]
     (IntegerSets/binarySearch coll key 0 (alength coll)))
  ([key begin end ^ints coll]
     (IntegerSets/binarySearch coll key begin end)))

(def ^:deprecated add-to-int-set conj)
(def ^:deprecated equals =)
