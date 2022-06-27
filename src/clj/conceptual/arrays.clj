(ns conceptual.arrays
  (:require [conceptual.timing]
            [clojure.data.fressian :as fressian])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio ByteBuffer]))

(def object-array-class (Class/forName "[Ljava.lang.Object;"))
(def int-array-class (Class/forName "[I"))
(def byte-array-class (Class/forName "[B"))
(def keyword-array-class (Class/forName "[Lclojure.lang.Keyword;"))

(def ^:const STRING 0)
(def ^:const INTEGER 1)
(def ^:const LONG 2)
(def ^:const FRESSIAN 3)

(defn int-array?
  "Returns true is the given parameter is an int array."
  [obj] (instance? int-array-class obj))

(defn byte-array?
  "Returns true is the given parameter is an byte array."
  [obj] (instance? byte-array-class obj))

(defn object-array?
  "Returns true is the given parameter is an int array."
  [obj] (instance? object-array-class obj))

(defn print-bytes [^bytes bytes]
  (apply str (interpose " " bytes)))

(defn str->bytes [^String s]
  (let [b (.getBytes s "UTF-8")
        len (alength b)
        bb (ByteBuffer/allocate (inc len))]
    (.put bb (byte STRING))
    (.put bb b 0 len)
    (.array bb)))

(comment (conceptual.timing/ntimes 1000000 (print-bytes (str->bytes "hello")))
         (conceptual.timing/ntimes 1000000 (print-bytes (str->bytes "-tag"))))

(defn int->bytes [i]
  (let [bb (ByteBuffer/allocate 5)]
    (.put bb (byte INTEGER))
    (.putInt bb i)
    (.array bb)))

(comment (conceptual.timing/ntimes 1000000 (print-bytes (int->bytes 1)))
         (conceptual.timing/ntimes 1000000 (print-bytes (int->bytes 2)))
         (conceptual.timing/ntimes 1000000 (print-bytes (int->bytes Integer/MAX_VALUE))))

(defn long->bytes [i]
  (let [bb (ByteBuffer/allocate 9)]
    (.put bb (byte LONG))
    (.putLong bb i)
    (.array bb)))

(comment (conceptual.timing/ntimes 1000000 (print-bytes (long->bytes 1)))
         (conceptual.timing/ntimes 1000000 (print-bytes (long->bytes 2)))
         (conceptual.timing/ntimes 1000000 (print-bytes (long->bytes Long/MAX_VALUE))))

(defn object->bytes [o]
  (let [os (ByteArrayOutputStream.)
        writer (fressian/create-writer os)]
    (.write os FRESSIAN)
    (.writeObject writer o)
    (.toByteArray os)))

(comment (print-bytes (object->bytes "hello")))

(defn ->bytes [v]
  (cond (string? v) (str->bytes v)
        (integer? v) (cond
                      (instance? Integer v) (int->bytes v)
                      (instance? Long v) (long->bytes v))
        :else (object->bytes v)))

(comment (print-bytes (->bytes {:a "hello"})))

(defn bytes-> [^bytes bytes]
  (let [len (alength bytes)]
    (case (long (aget bytes 0))
      0 (String. bytes 1 (dec len))
      1 (.getInt (ByteBuffer/wrap bytes 1 4))
      2 (.getLong (ByteBuffer/wrap bytes 1 8))
      3 (let [is (ByteArrayInputStream. bytes 1 (dec len))
              reader (fressian/create-reader is)]
          (.readObject reader)))))

(comment (conceptual.timing/many (bytes-> (->bytes "hello")))
         (conceptual.timing/many (bytes-> (->bytes (int 1))))
         (conceptual.timing/many (bytes-> (->bytes (long 1)))))

(comment (bytes-> (->bytes "hello"))
         (bytes-> (->bytes {:a 1 :b "hello"}))
         (class (bytes-> (->bytes (int 1))))
         (class (bytes-> (->bytes (long 1)))))

(defn ensure-bytes [v]
  (if (byte-array? v) v
      (->bytes v)))
