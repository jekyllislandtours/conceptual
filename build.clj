(ns build
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]
            [clojure.pprint :as pprint]))

(def lib 'org.clojars.jekyllislandtours/conceptual)
(def version (format "0.1.%s" (b/git-count-revs nil)))

(defn show-defaults [_]
  (println "default-basis:")
  (pprint/pprint (bb/default-basis))
  (println "default-target:" (bb/default-target))
  (println "default-class-dir:" (bb/default-class-dir))
  (println "default-jar-file:" (bb/default-jar-file lib version)))

(defn clean [_]
  (bb/clean {}))

(defn compile-java [_]
  (print "Compiling java...")
  (b/javac {:src-dirs ["src/java"]
            :class-dir (bb/default-class-dir)
            :basis (bb/default-basis)
            :javac-opts [;;"-source" "8" "-target" "8"
                         "-Xlint:deprecation" "-Xlint:unchecked"]}))

(defn jar [_]
  (compile-java nil)
  (bb/jar {:lib lib
           :version version
           :src-dirs ["src/clj"]}))

(defn install [_]
  (jar nil)
  (println "Installing jar into local Maven repo cache...")
  (bb/install {:lib lib
               :version version
               :src-dirs ["src/clj"]}))

(defn deploy [_]
  (bb/deploy {:lib lib
              :version version
              :src-dirs ["src/clj"]}))
