Conceptual 0.1.0
================

Conceptual is a fast JVM based in-memory concept database that can be used as a feature store, graph database, and more.

#### Philosphy
  * maximize read performance - optimize all tradeoffs for read performance
  * generalized ontological/type independent data store
  * manage feature space as namespaced feature grammars
  * manage medium size (< 1 TB) kernel of data
  * heavier artifacts stored in out of band disk based indexes
  * multi-threaded reads/single-threaded writes

#### Tradeoffs
  * non-durable (although there are durable implementations)
  * does not support distributed transactions
  * does not currently index by value
  * does not keep track of changes to values through time

#### Architectural Design
  * in-memory
  * low-level kernel
  * embeddable
  * simple declarative Clojure dsl
  * functionally an EAV/AEV triple store
  * row/column based indexing
  * can support many different backing stores
  * default implementation represented as co-indexed arrays

### Usage

Add to :deps in your deps.edn:

```clojure
conceptual {:mvn/version "0.1.0"}
```

Once published to Clojars it will be:

```clojure
org.clojars.jekyllislandtours/conceptual {:mvn/version "0.1.32"}
```


Add to your dependencies in your Leiningen project.clj:

``` clojure
[conceptual "0.1.32"]
```

Once published to clojars it will be:

``` clojure
[org.clojars.jekyllislandtours/conceptual "0.1.32"]
```

### Building

Cleaning:

``` shell
clj -T:build clean
```

Compiling java code:

``` shell
clj -T:build compile-java
```

Building a jar:

``` shell
clj -T:build jar
```

Installing into local Maven repo cache:

``` shell
clj -T:build install
```

### Running the walkthrough and IMDb demo

Start the REPL with 25GB of memory:

``` shell
clj -Mwalkthrough
```

To go through the walkthrough just start evaluating each form one by one.

Then eval the demo/imdb_demo.clj namespace, then begin to eval the code at the bottom in the comment.


## Graph of Concepts

Conceptual indexes sets of concepts, think "entities" if it helps, that start with meta-models and work their way into concrete instances. Concepts themselves can be modeled as simple maps of key value pairs. Conceptual uses integers to keep track of concepts as well as the properties and relations--also concepts--of those concepts. Every concept will therefor have a key called `:db/id` which will contain the keyword for that concept.

#### Keywords

Conceptual automatically and supports Keywords as a unique indentifier. Every concept will therefor have a key called `:db/key` which will contain the keyword for that concept. The `db` before `key` is a namespace prefix used to denote Conceptual keys so that domain specific keywords will not collide. This kind of key is human readable and serves as the kind of key to be used and referred to outside of the system.

Here are some sample keywords:

```clojure
:dbpedia/The_Beatles
:baseball/batting-average
:msft/2015-04-06.closing-price
```

Keywords have a name and can have an optional namespace. In the last example `:dbpedia/The_Beatles`, `dbpedia` is the namespace and `The_Beatles` is the name. The leading `:` is not part of the namespace or the name.

```clojure
(namespace :dbpedia/The_Beatles)
=> "dbpedia"

(name :dbpedia/The_Beatles)
=> "The_Beatles"
```

### API Functions

#### Basic API

* seek - looks up a concept by `:db/id` or `:db/key`. The item return will be functionally equivalent to a clojure map.
* value - looks up a value by the concept id and property id either in the form of a `:db/id` or `:db/key`.
* ids - returns the 'db/ids' for concept given a `:db/id` or `:db/key`

#### Convenience API

* project
* idents
* scan

#### Sorted Integer Set Operations

* i/union
* i/intersection
* i/difference

#### Prefix Indices

* word-fixes/ids-by-prefix-phrase

#### Tupl General Purpose Indices

* load

### Values Types

* String
* Integer
* Long
* Double
* Float
* Boolean
* Primitive Arrays
* URI
* UUID
* Clojure data-types
* Date/Instant

### Types of Concepts

Concepts are used to model everything in the system from entities, attributes, categories, predicates, functions, relations, rules, etc.

#### Entities

Nouns - person, place or thing... id...

* The Beatles
* Barack Obama
* Federal Reserve Bank

#### Property/Attribute

Concepts can have properties or attributes which are the most common kind of data. In Conceptual properties or attributes are also concepts.

Types of information that can be modeled into property/attribute concepts:

* default sort order
* property words
* property components (int-arrays) as concepts
* min/max/default value
* comparator (if necessary)
* description
* types that have said property
* functions associated with derived properties

#### Tags

Tags themselves can be modeled as first class concepts. Tags are by convention followed by a `?` for example the tag to denote a tag is `:db/tag?`.

* usually have a boolean value, but just the presence of the tag is used to denote membership, the value is typically ignored.
* can contain any constraints or predicates which must be true before assigning tag.

#### Relations

Relations are properties that relate a concept to one or more other concepts. These relations are modeled as sorted integer arrays and refer to the `db/id` of the item(s) in that relation.

For relations the `db/relation?` tag will be true as will either of the `db/to-one-relation?` or `db/to-many-relation?` tags.

##### Built-in Relations

All properties or attributes have a built-in relations `db/ids` that is a

### Querying Concepts

#### seek

The function `seek` is the most common means of accessing concepts within Conceptual. Seek is overloaded in several ways that make it very versatile. Seek return either a map of KV pairs or it returns a single value depending on the arguments.

##### seeking by key

``` clojure
(seek :dbpedia/The_Beatles)
```

##### seeking by id

In addition to seeking by Keyword, it is also possible to seek by id:

```clojure
(seek 0)
=>
{:db/id 0,
 :db/key :db/id,
 :db/type java.lang.Integer,
 :db/property? true,
 :db/ids
 [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
  20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, ...]}
```

##### obtaining values from seek

When seek is given two parameters, it will return a singular value. What is required with the key or id for the concept and a key or id for the value being requested.

```clojure
(seek :dbpedia/The_Beatles :dbpedia/active-from)
=> 1960
```

This is of course available in integer format for the concept ids as well. In the following example 0 is the concept id for `:db/id` and 1 is the concept id for ":-key", thus the following is looking for the `:db/key` of `:db/id`.

```clojure
(seek 0 1)
=>
:db/id
```

#### Sets and Set Operations

Since concepts are represented as integers, sets of concepts can be represented by arrays of integers. Additional benefits occur when these integer arrays are sorted (i.e. binary search & O(m+n) union, intersection, and difference). Since columns/keys/properties themselves contain the `:db/ids` of those concepts that have them, you can use them as sets of concepts--wonderful!

##### ids

```clojure
(:db/ids (seek :db/tag?))
=> #<int[] I]7ee5d3be> ;; an int array
```

Since `seek` allows concept ids as a parameter you can of course `map seek` across sets of concepts as int arrays.

```clojure
(map seek (ids :db/tag?))
=>
```

##### intersection

Concepts present in both of two different A and B sets can be found using the `intersection` function.

```clojure
(intersection (ids :mbti/thinking?)
              (ids :mbti/introverted?))
=> #<int[] I]5ff2d3ba>
```

##### union

Concepts present in either of two different sets A and B can be found using the `union` function.

```clojure
(union <int-array-A> <int-array-B>)
=> <int-array-C>
```

So for example:

```clojure
(union (ids :mbti/thinking?)
       (ids :mbti/introverted?))
```

##### difference

```clojure
(difference (ids :mbti/thinking?)
            (ids :mbti/introverted?))
```

#### Projections

The following code will return a lazy list of vectors of `[:db/id :db/key]`:

```clojure
(->> (ids :tag?)
     (map seek)
     (map #(vector (:db/id %) (:db/key %)))
```

A faster, albeit non-lazy, way of doing the projection above is the following:

```clojure
(project [:db/id :db/key]
         (ids :db/tag?))
```

#### Filtering

Use the built-in `filter` provided by Clojure.

The following counts concepts who have `:baseball/batting-average` > than 3.0. Note that we are using the set of `:-ids` associated with the concept of `:wpi/unlikely-virtues` to filter on.

```clojure
(->> (ids :baseball/batting-average)
     (map seek)
     (filter #(> (:baseball/batting-average %) 3.0))
     (count))
=> 252
```

Projections can also be filtered similarly. The following projection filters entries where `:wpi/self-control` is not nil. Note composing juxt with seek will also work of course.

```clojure
(->> (ids :dbpedia/American_child_actors)
     (project [:db/key :wpi/self-control])
     (filter (comp not nil? second))
     (sort-by second)
=>
((:dbpedia/corey.haim 1.0)
 (:dbpedia/dana.plato 1.0)
 (:dbpedia/gary.coleman 1.0)
 (:dbpedia/macaulay.culkin 1.0)
 ...)
```

By choosing `:wpi/self-control` as our set instead of `:dbpedia/American_child_actors`.

```clojure
(->> (ids :wpi/self-control)
     (project [:db/key :/wpi/self-control])
     (filter (comp (partial > 5.0) second)))
```
