(ns conceptual.int-sets
  (:refer-clojure :exclude [contains? conj disj dedupe keep mapcat set sort])
  (:import (conceptual.util IntegerSets IntArrayList)
           (java.util Arrays)))


(set! *warn-on-reflection* true)

(def +empty+ IntegerSets/EMPTY)


;;(def a (int-array [1 2 3 4 5 6 7]))
;;(seq a)
;;(def b (int-array [3 4 5 6 7 8 9]))
;;(seq b)
;;(def c (int-array [0 1 2]))
;;(def d (int-array [5 8 10 15]))
;;(def test-set (let [size 10] (seq (mapv (fn [n] (int-array (range n (+ n size)))) (range 0 size)))))

(defn- chunked-reduce
  "Acts similarly to reduce except f is passed chunk-size number of
  arguments from coll after val. For example (chunked-reduce + 0 [1 2
  3 4 5 6 7 8] 3) will pass 0, 1, 2, and 3 to + in the first
  iteration."
  [f val coll chunk-size]
  (loop [current-coll coll
         acc val]
    (let [result (apply f acc (take chunk-size current-coll))
          rest (drop chunk-size current-coll)]
      (if (seq current-coll)
        (recur rest
               result)
        result))))

(defn intersection
  "Returns the integer intersection of the given sorted int array sets."
  ([] +empty+)
  ([^ints a] (or a +empty+))
  ([^ints a ^ints b] (IntegerSets/intersection a b))
  ([^ints a ^ints b ^ints c] (IntegerSets/intersection a b c))
  ([^ints a ^ints b ^ints c ^ints d] (IntegerSets/intersection a b c d))
  ([^ints a ^ints b ^ints c ^ints d ^ints e] (IntegerSets/intersection a b c d e))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f] (IntegerSets/intersection a b c d e f))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g] (IntegerSets/intersection a b c d e f g))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h] (IntegerSets/intersection a b c d e f g h))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h & more]
   (chunked-reduce intersection (intersection a b c d e f g h) more 7)))

;;(seq (intersection a b))
;;(seq (apply intersection test-set))

(defn union
  "Returns the integer union of the given sorted int array sets."
  ([] +empty+)
  ([^ints a] (or a +empty+))
  ([^ints a ^ints b] (IntegerSets/union a b))
  ([^ints a ^ints b ^ints c] (IntegerSets/union a b c))
  ([^ints a ^ints b ^ints c ^ints d] (IntegerSets/union a b c d))
  ([^ints a ^ints b ^ints c ^ints d ^ints e] (IntegerSets/union a b c d e))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f] (IntegerSets/union a b c d e f))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g] (IntegerSets/union a b c d e f g))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h] (IntegerSets/union a b c d e f g h))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h & more]
     (chunked-reduce union (union a b c d e f g h) more 7)))

;;(seq (union a b))
;;(seq (apply union test-set))

(defn difference
  "Returns the integer difference of the given sorted int arrays. Everything is
  subtracted from the first sorted int array set."
  ([] +empty+)
  ([^ints a] (or a +empty+))
  ([^ints a ^ints b] (IntegerSets/difference a b))
  ([^ints a ^ints b ^ints c] (difference a (union b c)))
  ([^ints a ^ints b ^ints c ^ints d] (difference a (union b c d)))
  ([^ints a ^ints b ^ints c ^ints d ^ints e] (difference a (union b c d e)))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f] (difference a (union b c d e f)))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g] (difference a (union b c d e f g)))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h] (difference a (union b c d e f g h)))
  ([^ints a ^ints b ^ints c ^ints d ^ints e ^ints f ^ints g ^ints h & more]
   (difference a (apply union a b c d e f g h more))))

(defn add-to-int-set [^ints a b]
  (union a (int-array [b])))

;;(seq (difference (int-array (range 10 20)) (apply union test-set)))

(defn conj
  [^ints a b]
  (union a (int-array [b])))

(defn disj
  [^ints a b]
  (difference a (int-array [b])))

(defn index-of
  "Returns the index of a key in a sorted int array, -1 if not found."
  [key ^ints coll]
  (if key
    (IntegerSets/contains coll key)
    -1))

;;(index-of 3 a)


(defn member?
  "Returns `true` if `key` is in `coll`"
  [key ^ints coll]
  (not= -1 (index-of key coll)))


(defn contains?
  "Returns `true` if `key` is in `coll`"
  [^ints coll key]
  (member? key coll))


(defn binary-search-greater
  "Searches for a key in a sorted array, and returns an index to an element
   which is greater than or equal key. Returns end if nothing greater or equal
   was found, else an index satisfying the search criteria."
  ([key ^ints coll]
     (IntegerSets/binarySearchGreater coll key))
  ([key begin end ^ints coll]
     (IntegerSets/binarySearchGreater coll key begin end)))

;;(binary-search-greater 3 0 (alength a) a)
;;(binary-search-greater 3 2 (alength a) a)
;;(binary-search-greater 3 3 (alength a) a)
;;(binary-search-greater 3 a)
;;(binary-search-greater 2 c)
;;(binary-search-greater 20 d)

(defn binary-search-smaller
  "Searches for a key in a sorted array, and returns an index to an element
      which is smaller than or equal key."
  ([key ^ints coll] (IntegerSets/binarySearchSmaller coll key))
  ([key begin end ^ints coll]
     (IntegerSets/binarySearchSmaller coll key begin end)))

;;(binary-search-smaller 3 0 (alength a) a)
;;(binary-search-smaller 3 0 3 a)
;;(binary-search-smaller 3 0 2 a)
;;(binary-search-smaller 0 a)
;;(binary-search-smaller 7 a)

(defn binary-search
  "Searches for a key in a sorted array."
  ([key ^ints coll]
     (IntegerSets/binarySearch coll key 0 (alength coll)))
  ([key begin end ^ints coll]
     (IntegerSets/binarySearch coll key begin end)))

(defn equals?
  "Returns true if a and b are equal in contents."
  [^ints a ^ints b]
  (IntegerSets/equals a b))

(def ^:deprecated equals equals?)


(defn subset?
  "Is set1 a subset of set2?"
  [^ints set1 ^ints set2]
  (equals? (intersection set1 set2) set1))

(defn superset?
  "Is set1 a superset of set2?"
  [^ints set1 ^ints set2]
  (equals? (intersection set1 set2) set2))


(defn encode
  "Encodes and integer array into a byte array."
  [^ints s]
  (IntegerSets/encode s))

(defn decode
  "Decodes an encoded integer array back into into an integer array."
  [^bytes s]
  (IntegerSets/decode s))

(defn set
  [xs]
  (if (= int/1 (class xs))
    xs
    (IntArrayList/sortedIntSet xs)))


(defn sort
  "[ALPHA] Sorts an int array in place and returns the sorted array. NB. The input `xs` is
  modified in place and returned. This is not persistent and is meant to be a
  performance optimization."
  [^int/1 xs]
  (Arrays/sort xs) ;; this sorts in place
  xs)

(defn dedupe
  "[ALPHA] Dedupes a sorted int array. Returns the same array if there were no dupes which is
  not persistent and is meant to be a performance optimization."
  [xs]
  (IntArrayList/dedupe xs))

(defn- keep-int-set
  [f ^int/1 xs]
  (let [ans (IntArrayList/new)]
    (dotimes [i (alength xs)]
      (.add ans ^Number (f (aget xs i))))
    (.toSortedIntSet ans)))

(defn keep
  "[ALPHA] Returns an int set of applying `f` to `xs`. `f` must return
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
  "[ALPHA] Returns an int set of applying `f` to `xs`. `f` must return
  either `nil` or an integer array."
  [f xs]
  (if (= int/1 (class xs))
    (mapcat-int-set f xs)
    ;; we're not doing count since it might be a lazy seq
    (let [ans (IntArrayList/new)]
      (reduce (fn [^IntArrayList l id]
                (.addAll l ^int/1 (f id))
                l)
              ans
              xs)
      (.toSortedIntSet ans))))
