(ns conceptual.main
  (:require [conceptual.core :as core]
            [conceptual.word-fixes :as word-fixes]))

(def logo
  (str "Welcome to Conceptual\n"
       "Have a principled hacking session and may all your dreams come true!\n"))

(defn -main [& args]
  (time
   (do
     (println "Starting Conceptual.")
     (println "Loading pickle... ")
     (print "    ")

     (time (core/load-pickle! :filename "pickle.gz" :type :r))
     (println "Reticulating splines... ")
     (time (word-fixes/build-prefix-index))
     (println)
     (println logo)
     (print "Total "))))

;;(-main)
;;(core/sanity-check)
