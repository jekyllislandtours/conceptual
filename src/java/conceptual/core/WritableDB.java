package conceptual.core;

public interface WritableDB extends DB {
    /**
     * Inserts a set of keys and values into the DB.
     * Assigns a unique internal :db/id to every concept inserted.
     *
     * @param ks a sparse array of integer keys.
     * @param vs a sparse co-indexed array of values corresponding to ks.
     * @return WritableDB a new version of the database with the new concept.
     */
    WritableDB insert(final int[] ks, final Object[] vs);

    /**
     * Inserts a set of keys and values into the DB.
     * Assigns a unique internal :db/id to every concept inserted.
     *
     * @param aggregator an object responsible for tracking keys for a concept to be
     *                   efficiently indexed.
     * @param ks a sparse array of integer keys.
     * @param vs a sparse co-indexed array of values corresponding to ks.
     * @return WritableDB a new version of the database with the new concept.
     */
    WritableDB insert(final IndexAggregator aggregator, final int[] ks, final Object[] vs);

    /**
     * Updates a value for a key in a concept. If the key does not exist it is added.
     *
     * @param id the id (:db/id) of the concept.
     * @param key the key corresponding to the value
     * @param val the value corresponding to the key.
     * @return a new version of the database with the new concept.
     */
    WritableDB update(final int id, final int key, final Object val);

    /**
     * Updates a value for a key in a concept. If the key does not exist it is added.
     *
     * @param aggregator an object responsible for tracking keys for a concept to be
     *                   efficiently indexed.
     * @param id the id (:db/id) of the concept.
     * @param key the key corresponding to the value
     * @param val the value corresponding to the key.
     * @return a new version of the database with the new concept.
     */
    WritableDB update(final IndexAggregator aggregator, int id, int key, Object val);

    /**
     * Updates keys and values in a concept. If the key does not exist it is added.
     *
     * @param aggregator an object responsible for tracking keys for a concept to be
     *                   efficiently indexed.
     * @param id the id (:db/id) of the concept.
     * @param keys the keys corresponding to the values. (co-indexed)
     * @param vals the values corresponding to the keys. (co-indexed)
     * @return a new version of the database with the new concept.
     */
    WritableDB update(final IndexAggregator aggregator, final int id,
                      final int[] keys, final Object[] vals);


    /**
     * Updates a contiguous set of keys and values in the DB.
     * NOTE: this method can cause problems if you don't know what your doing
     *       as there are no checks.
     * TODO: make this safer.
     *
     * @param aggregator an object responsible for tracking keys for a concept to be
     *                   efficiently indexed.
     * @param id the id (:db/id) of the concept.
     * @param ks a sparse array of integer keys.
     * @param vs a sparse co-indexed array of values corresponding to ks.
     * @return WritableDB a new version of the database with the new concept.
     */
    WritableDB updateInline(final IndexAggregator aggregator, int id, final int[] ks, final Object[] vs);

}
