(ns conceptual.core.crypto-test
  (:require [conceptual.core :as c]
            [conceptual.schema :as s]
            [conceptual.int-sets :as i]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Keyword PersistentHashMap]
           [java.io FileOutputStream FileInputStream
            PrintWriter]
           [java.util Date]
           [java.time Instant]
           [javax.crypto Cipher KeyGenerator SecretKey
            CipherInputStream
            CipherOutputStream]
           [java.security SecureRandom]
           [conceptual.core DBTranscoder RDB]))


(defn bytes->str [bytes]
  (println "count:" (count bytes))
  (map identity bytes))


(deftest cipher-stream-test
  (testing "Cipher Streams"
    (let [kg (KeyGenerator/getInstance "AES")
          _ (.init kg (SecureRandom. (byte-array [7 2 3])))
          key (.generateKey kg)
          cipher (.. (Cipher/getInstance "AES")
                     (init Cipher/ENCRYPT_MODE key))

          decipher (.. (Cipher/getInstance "AES")
                       (init Cipher/DECRYPT_MODE key))
          _ (println "format: " (.getFormat key))
          _ (println "encoded: " (bytes->str (.getEncoded key)))
          fos (FileOutoutStream. "cipher.txt")
          fis (FileInputStream. "cipher.txt")
          cos (CipherOutputStream. fos)
          pw (PrintWriter. cos)
          _ (.println pw "hello world")
          _ (.flush pw)
          _ (.close pw)

          cis (CipherInputStream. fis decipher)
          ]
      (is true)
      )))


(deftest pickle-encryption-test
  (testing "Pickle Encryption Test"
    (let [pickle-path "temp/test_pickle.enc"
          kg (KeyGenerator/getInstance "AES")
          _ (.init kg (SecureRandom. (byte-array [7 2 3])))
          key (.generateKey kg)
          cipher (.. (Cipher/getInstance "AES")
                     (init Cipher/ENCRYPT_MODE key))

          decipher (.. (Cipher/getInstance "AES")
                       (init Cipher/DECRYPT_MODE key))
          ]
      ;; ensure pickle path
      (io/make-parents pickle-path)

      ;; pickle setup
      (c/create-db!)
      (is (= 13 (c/max-id)))

      ;; compact the PersistentDB into an RDB type
      (c/compact!)
      (is (= 13 (c/max-id)))

      ;; type should be RDB
      (is (instance? conceptual.core.RDB (c/db)))

      ;; pickle round-trip
      (c/pickle! :filename pickle-path :cipher cipher)
      (c/load-pickle! :filename pickle-path :cipher decipher)
      (is (= 13 (c/max-id)))

      ;;(io/delete-file pickle-path)
      )))
