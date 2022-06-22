# TODO

# Prepare initial release

* DONE replace dates with latest java Instants
* DONE remove wharf
* DONE Fix Tupl dependencies
* DONE use nippy to encode/decode EDN
* DONE remove Joda DateTime dependency
* IN-PROGRESS README.md
* IN-PROGRESS basic walkthrough
* IN-PROGRESS deep-dive imdb demo
* implement unsupported fn's in Tupl
* replace/add Fressian with Nippy
* fix javac warnings
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
  * test every type transcoding round-trip
  * core namespace test functions
  * schema namespace test functions
* version
* publish to clojars

# Future Versions
* [RoaringBitmap implementation](https://github.com/RoaringBitmap/RoaringBitmap)
* ByteDB implementation
* ByteTuplDB implementation
* FirestoreDB implementation
* DatomicDB implementation
