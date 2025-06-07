(ns conceptual.core.crypto-test
  (:require
   [conceptual.core :as c]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest testing]]
   [expectations.clojure.test :refer [expect]])
  (:import
   (java.io FileOutputStream FileInputStream
    PrintWriter
    InputStreamReader
    BufferedReader)
   (javax.crypto Cipher KeyGenerator
    CipherInputStream
    CipherOutputStream)
   (java.security SecureRandom)))


(defn bytes->str [bytes]
  (map identity bytes))


(deftest cipher-stream-test
  (testing "Cipher Streams"
    (let [kg (KeyGenerator/getInstance "AES")
          _ (.init kg (SecureRandom. (byte-array [7 2 3])))
          key (.generateKey kg)
          cipher (Cipher/getInstance "AES")
          _ (.init cipher Cipher/ENCRYPT_MODE key)

          decipher (Cipher/getInstance "AES")
          _ (.init decipher Cipher/DECRYPT_MODE key)
          fos (FileOutputStream. "cipher.txt")
          fis (FileInputStream. "cipher.txt")
          cos (CipherOutputStream. fos cipher)
          pw (PrintWriter. cos)
          _ (.println pw "hello world")
          _ (.flush pw)
          _ (.close pw)

          cis (CipherInputStream. fis decipher)
          isr (InputStreamReader. cis)
          br (BufferedReader. isr)
          line (.readLine br)]
      (expect "hello world" line)
      (io/delete-file "cipher.txt"))))


(deftest pickle-encryption-test
  (testing "Pickle Encryption Test"
    (let [pickle-path "temp/test_pickle.aes.sz"
          kg (KeyGenerator/getInstance "AES")
          _ (.init kg (SecureRandom. (byte-array [7 2 3])))
          key (.generateKey kg)
          cipher (Cipher/getInstance "AES")
          _ (.init cipher Cipher/ENCRYPT_MODE key)

          decipher (Cipher/getInstance "AES")
          _ (.init decipher Cipher/DECRYPT_MODE key)
          ]
      ;; ensure pickle path
      (io/make-parents pickle-path)

      ;; pickle setup
      (c/create-db!)
      (expect 13 (c/max-id))

      ;; compact the PersistentDB into an RDB type
      (c/compact!)
      (expect 13 (c/max-id))

      ;; type should be RDB
      (expect true (instance? conceptual.core.RDB (c/db)))

      ;; pickle
      (c/pickle! :filename pickle-path
                 :cipher cipher)

      ;; reset then load the pickle
      (c/reset-pickle! :filename pickle-path
                       :cipher decipher)
      (expect 13 (c/max-id))

      (io/delete-file pickle-path))))
