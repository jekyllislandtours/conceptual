(ns conceptual.uuid)

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

(defn- to-hex-string [n l]
  (let [s (format "%x" n)
        c (count s)]
    (cond
     (> c l) (subs s 0 l)
     (< c l) (str (apply str (repeat (- l c) "0")) s)
     :else s)))

(defn squuid []
  (str
   (-> (org.joda.time.DateTime.) (.getMillis) (/ 1000.0) (Math/round) (to-hex-string 8))
   "-" (-> (rand-bits 16) (to-hex-string 4))
   "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
   "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
   "-" (-> (rand-bits 16) (to-hex-string 4))
   (-> (rand-bits 16) (to-hex-string 4))
   (-> (rand-bits 16) (to-hex-string 4))))

(defn squuid-time-millis [uuid]
  (-> (subs uuid 0 8)
      (Integer/parseInt 16)
      (* 1000)))

;;(squuid)
