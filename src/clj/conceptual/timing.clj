(ns conceptual.timing)

(defmacro ntimes [n & forms]
  `(loop [res# nil
          total# 0.0
          i# 0]
     (if (< i# ~n)
       (let [start# (System/nanoTime)
             r# (do ~@forms)]
         (recur (if (seq? r#)
                  (doall r#)
                  r#)
                (+ total# (- (System/nanoTime) start#))
                (inc i#)))
       (format "%.3fms" (/ total# (* 1000000.0 ~n))))))

(defmacro many [& forms]
  `(ntimes 1000 ~@forms))

(defmacro timev
  "Evaluates expr and measures the time it took.
   Returns a vector of the value of expr and the time in ms."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     [ret# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]))

(comment (timev "hello"))

(defmacro duration
  "Evaluates expr and measures the time it took.
   Returns a the time in ms, discards results of expr."
  [expr]
  `(let [start# (. System (currentTimeMillis))
         ret# ~expr]
     (- (. System (currentTimeMillis)) start#)))

(comment (duration "hello"))
