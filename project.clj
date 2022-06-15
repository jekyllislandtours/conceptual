(defproject conceptual "0.1.0"

  :description "Concept store"

  :repositories [["jitpack.io" "https://jitpack.io"]]

  ;; TODO: review dependencies
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.int-map "1.0.0"]
                 ;; TODO replace
                 [clj-time "0.15.2"]
                 [com.cognitect/transit-clj "1.0.329"]
                 [org.clojure/data.fressian "0.2.1"]
                 [org.cojen/cojen "2.2.5"]
                 [com.github.cojen/Tupl "1.5.3.3" :exclusions [net.java.dev.jna/jna-platform]]
                 ;;[org.cojen/tupl "1.4.0.1" :exclusions [net.java.dev.jna/jna-platform]]
                 [org.apache.commons/commons-pool2 "2.11.1"]
                 [org.apache.commons/commons-compress "1.10"]
                 [org.iq80.snappy/snappy "0.4"]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :min-lein-version "2.7.1"

  :jvm-opts ^:replace ~["-Dclojure.compiler.direct-linking=true"]

  ;;:javac-options ["-target" "1.9" "-source" "1.9" "-Xlint:-options"]

  :global-vars {*warn-on-reflection* true
                *assert* false}
  :aot [:all])
