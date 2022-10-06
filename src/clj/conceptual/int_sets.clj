(ns conceptual.int-sets
  (:refer-clojure :exclude [contains?])
  (:import [conceptual.util IntegerSets]))

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
  ([] (int-array []))
  ([^ints a] a)
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
  ([] (int-array []))
  ([^ints a] a)
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
  ([] (int-array []))
  ([^ints a] a)
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

(defn index-of
  "Returns the index of a key in a sorted int array, -1 if not found."
  [key ^ints coll]
  (IntegerSets/contains coll key))

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

(defn equals
  "Returns true if a and b are equal in contents."
  [^ints a ^ints b]
  (IntegerSets/equals a b))

(defn encode
  "Encodes and integer array into a byte array."
  [^ints s]
  (IntegerSets/encode s))

(defn decode
  "Decodes an encoded integer array back into into an integer array."
  [^bytes s]
  (IntegerSets/decode s))
