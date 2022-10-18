package conceptual.core;

import clojure.lang.Counted;
import clojure.lang.Keyword;

public interface DB extends Counted {

    int ID_ID = 0;
    int KEY_ID = 1;
    int TYPE_ID = 2;
    int PROPERTY_TAG_ID = 3;
    int TAG_TAG_ID = 4;
    int UNIQUE_TAG_ID = 5;
    int DONT_INDEX_TAG_ID = 6;
    int RELATION_TAG_ID = 7;
    int TO_MANY_RELATION_TAG_ID = 8;
    int TO_ONE_RELATION_TAG_ID = 9;
    int INVERSE_RELATION_TAG_ID = 10;
    int IDS_ID = 11;
    int FN_TAG_ID = 12;
    int FN_ID = 13;

    /**
     * Returns the identity of this database.
     * @return Keyword the keyword identity for this DB.
     */
    Keyword getIdentity();

    // cardinality

    /**
     * Returns the max id.
     * @return int the max id
     */
    int getMaxId();

    /**
     * Returns the number of concepts in the DB.
     * @return int the concept count.
     */
    int count();

    /**
     * Returns the number of triples in the DB.
     * @return int the count of triples in the DB.
     */
    int getTripleCount();

    /**
     * Returns the number of keys in the DB.
     * @return int the cound of keys in the DB.
     */
    int getKeyCount();

    // key to id

    /**
     * Returns the id for a given key.
     * @param key the unique key for a concept
     * @return Integer the Boxed integer corresponding to the given key
     */
    Integer keywordToId(Keyword key);

    /**
     * Returns the int id given a Keyword, String, Integer, or Long.
     *
     * @param key the unique key in Keyword, String, Integer or Long form.
     * @return int the concept id corresponding to the given key
     */
    int keyToId(Object key);

    // id to key

    /**
     * Returns true if the given concept (id) contains the given key.
     *
     * @param id the id for the concept.
     * @param key the key to check for.
     * @return boolean true if key exists, false otherwise.
     */
    boolean containsKey(int id, int key); // derivative

    /**
     * Given an id for a concept returns they Keyword representing the :db/key.
     * @param id the id for the concept.
     * @return Keyword the Keyword representing the given concept.
     */
    Keyword getKeyword(int id); // derivative

    /**
     * Return the keys for a given concept (id) as a Keyword array.
     *
     * @param id the id for a given concept.
     * @return Keyword[] the keywords for the keys for the given concept.
     */
    Keyword[] getKeysAsKeywords(int id); // derivative

    // keys

    /**
     * Returns the key ids for a given concept id.
     * @param id the id for a given concept.
     * @return int[] the ids of the concept's keys.
     */
    int[] getKeys(final int id);

    /**
     * Returns the index of the key in the set of sorted keys for a given concept id.
     *
     * WARNING: low-level interface.
     *
     * @param id the id for a given concept.
     * @param key the key to find the index of.
     * @return int the index of they key for the given concept.
     */
    int getKeyIdx(final int id, final int key);

    /**
     * Returns the key given a concept id and an index into it's keys.
     *
     * WARNING: low-level interface.
     *
     * @param id the id for a given concept.
     * @param idx the index into the keyspace for a given concept.
     * @return int the key at the given index for a given concept id.
     */
    int getKeyByIdx(final int id, final int idx);

    /**
     * Returns a Keyword fore the key at the given index for a given a concept.
     *
     * WARNING: low-level interface.
     *
     * @param id the id for a given concept.
     * @param idx the index into the keyspace for a given concept.
     * @return Keyword the Keyword for a the key at the given index for a given concept id.
     */
    Keyword getKeywordByIdx(final int id, final int idx);

    // values

    /**
     * Returns the set of values for a given concept. The result is a parallel
     * array to the array of keys given by getKeys.
     *
     * @param id the id for a given concept.
     * @return Object[] an array of values for the given concept.
     */
    Object[] getValues(final int id);

    /**
     * Returns the valude given a concept id and an index into the array of values.
     *
     * WARNING: low-level interface.
     *
     * @param id the the id for a given concept.
     * @param idx
     * @return
     */
    Object getValueByIdx(final int id, final int idx);

    /**
     * Returns a value given a concept id and a key. This methods is the real bread and butter
     * of this interface.
     *
     * @param id the id for a given concept.
     * @param key the key for the value desired.
     * @return Object the value given a concept id and key.
     */
    Object getValue(final int id, final int key);

    // higher level interfaces

    /**
     * Returns a map for a given concept.
     *
     * @param id the id for a given concept.
     * @return DBMap the map for a given concept.
     */
    DBMap get(int id);

    /**
     * Given a set of keys and a set of concept ids returns an array of values
     * corresponding to the given keys for each concept id.
     *
     * @param keys the keys to project.
     * @param ids the set of concept ids to project the given keys across.
     * @return Object[][] a result set of values corresponding to the key projection across
     *                    the given concept ids.
     */
    Object[][] project(int[] keys, int[] ids);

    // facade interfaces

    /**
     * Returns an array of KeyFrequencyPairs for the given keys
     * sorted by the frequency that the keys occur.
     *
     * @param ids
     *         the key ids to be sorted by frequency within the id set.
     * @return KeyFrequencyPair[] the set of keys with frequency sorted by frequency.
     */
    KeyFrequencyPair[] getKeysByFrequency(int[] ids);

    /**
     * Returns an array of KeyFrequencyPairs for the given keys
     * sorted by the frequency that the keys occur.
     *
     * @param ids
     *         the key ids to be sorted by frequency within the id set.
     * @param skipKeys
     *         the set of keys to skip and not return in the result.
     * @return KeyFrequencyPair[] the set of keys with frequency sorted by frequency.
     */
    KeyFrequencyPair[] getKeysByFrequency(int[] ids, int[] skipKeys);

    /**
     * Returns an array of KeyFrequencyPairs for the given keys within the
     * specified relationKey sorted by the frequency that the keys occur.
     *
     * Ex. If the relationKey is :categories then you will get that set of
     * categories returned by frequency within the given id set.
     *
     * @param ids
     *         the key ids to be sorted by frequency within the id set.
     * @param relationKey
     *         the relation (i.e. :categories) to count by frequency.
     * @return KeyFrequencyPair[] the set of keys in the relationKey with frequency sorted by frequency.
     */
    KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey);

    /**
     * Returns an array of KeyFrequencyPairs for the given keys within the
     * specified relationKey sorted by the frequency that the keys occur.
     *
     * Ex. If the relationKey is :categories then you will get that set of
     * categories returned by frequency within the given id set.
     *
     * @param ids
     *         the key ids to be sorted by frequency within the id set.
     * @param relationKey
     *         the relation (i.e. :categories) to count by frequency.
     * @param skipKeys
     *         the set of keys to skip and not return in the result.
     * @return KeyFrequencyPair[] the set of keys in the relationKey with frequency sorted by frequency.
     */
    KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey, int[] skipKeys);

    void shutdown();

}
