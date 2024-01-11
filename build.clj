(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]
            [clojure.pprint :as pprint]))

(def lib 'org.clojars.jekyllislandtours/conceptual)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn show-defaults [_]
  (println "default-basis:")
  (pprint/pprint @basis)
  (println "target:" "target")
  (println "class-dir:" class-dir)
  (println "jar-file:" jar-file))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-java [_]
  (println "Compiling java...")
  (b/javac {:src-dirs ["src/java"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "11"
                         "-Xlint:deprecation" "-Xlint:unchecked"
                         "-proc:none"]}))


(defn compile-clj [_]
  (println "Compiling clj...")
  (b/compile-clj {:src-dirs ["src/clj"]
                  :class-dir class-dir
                  :basis @basis}))


(defn- pom-template [version]
  [[:description "Conceptual is a fast JVM based in-memory concept database that can be used as a feature store, graph database, and more."]
   [:url "https://github.com/jekyllislandtours/conceptual"]
   [:licenses
    [:license
     [:name "MIT License"]
     [:url "https://opensource.org/license/mit/"]]]
   [:developers
    [:developer
     [:name "Scott Jappinen"]]]
   [:scm
    [:url "https://github.com/jekyllislandtours/conceptual"]
    [:connection "scm:git:https://github.com/jekyllislandtours/conceptual.git"]
    [:developerConnection "scm:git:ssh:git@github.com:jekyllislandtours/conceptual.git"]
    [:tag (str "v" version)]]])

(defn jar [_]
  (clean nil)
  (compile-java nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src/clj"]
                :pom-data (pom-template version)})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install [_]
  (jar nil)
  (println "Installing jar into local Maven repo cache...")
  (b/install {:lib lib
              :version version
              :src-dirs ["src/clj"]}))

(defn deploy [_]
  (dd/deploy {:installer :remote
              :artifact (b/resolve-path jar-file)
              :pom-file (b/pom-path {:lib lib
                                     :class-dir class-dir})}))
