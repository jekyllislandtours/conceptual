# Conceptual TODO

# Prepare initial release

* DONE replace dates with latest java Instants
* DONE remove wharf
* DONE Fix Tupl dependencies
* DONE use nippy to encode/decode EDN
* DONE remove Joda DateTime dependency
* DONE fix javac warnings
* DONE support DB and IndexAggregator arities in schema
* DONE clj-kondo fixes
* IN-PROGRESS README.md
* IN-PROGRESS basic walkthrough
* IN-PROGRESS deep-dive imdb demo
* fix arity and refactor triples namespace
* refactory tupl-db namespace
* implement unsupported fn's in Tupl
* replace/add Fressian with Nippy
* remove array types in favor of edn
* support more types and/or delegate more to Nippy
  * Instant
  * Duration
  * Period
  * clojure Record/Type
  * URI
  * UUID
  * custom types
* unit tests
  * DONE test most types transcoding round-trip
  * core namespace test functions
  * schema namespace test functions
  * create tests from the examples in the int-sets namespace
* DONE version
* DONE upgrade build.clj to support install and deploy with sean cornfields build.clj
* DONE make repo public
* DONE publish to clojars
* build on commit to main

# Future Versions
* [RoaringBitmap implementation](https://github.com/RoaringBitmap/RoaringBitmap)
* ByteDB implementation
* ByteTuplDB implementation
* FirestoreDB implementation
* DatomicDB implementation
