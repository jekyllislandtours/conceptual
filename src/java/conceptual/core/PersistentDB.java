package conceptual.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;

import conceptual.util.IntArrayPool;
import conceptual.util.IntegerSets;

public final class PersistentDB implements WritableDB {

    final Keyword identity;
    final IPersistentMap uniqueIndices;
    final IPersistentMap keyIndex;
    final IPersistentMap valIndex;
    final int maxId;

    public PersistentDB(Keyword identity,
                        IPersistentMap uniqueIndices,
                        IPersistentMap keyIndex,
                        IPersistentMap valIndex,
                        int maxId) {
        this.identity = identity;
        this.uniqueIndices = uniqueIndices;
        this.keyIndex = keyIndex;
        this.valIndex = valIndex;
        this.maxId = maxId;
    }

    @Override
    public Keyword getIdentity() { return identity; }

    @Override
    public int getMaxId() {
        return maxId;
    }

    @Override
    public int count() { return maxId + 1; }

    @Override
    public int getTripleCount() {
        int result = 0;
        for (int i=0; i < valIndex.count(); i++) {
            Object[] vals = getValues(i);
            for (int j=0; j < vals.length; j++) {
                Object tmp = vals[j];
                if (tmp != null && tmp instanceof int[]) {
                    result += ((int[]) tmp).length;
                } else {
                    result++;
                }
            }
        }
        return result;
    }

    @Override
    public int getKeyCount() {
        int result = 0;
        for (int i=0; i < keyIndex.count(); i++) {
            result += getKeys(i).length;
        }
        return result;
    }

    @Override
    public int[] getKeys(final int id) {
        return (int[]) keyIndex.valAt(id, null);
    }

    @Override
    public Object[] getValues(final int id) {
        return (Object[]) valIndex.valAt(id, null);
    }

    @Override
    public Integer keywordToId(Keyword key) {
        Integer result = null;
        // TODO: ensure keyIndex exists so this check isn't necessary
        IPersistentMap keyIndex = (IPersistentMap) uniqueIndices.valAt(DB.KEY_ID);
        if (keyIndex != null) {
            result = (Integer) keyIndex.valAt(key);
        }
        return result;
    }

    @Override
    public int keyToId(Object key) {
        int kid = -1;
        if (key != null) {
            if (key instanceof Keyword) {
                kid = keywordToId((Keyword) key);
            } else if (key instanceof String) {
                kid = keyToId(Keyword.intern((String) key));
            } else if (key instanceof Integer) {
                kid = (Integer) key;
            } else if (key instanceof Long) {
                kid = ((Long) key).intValue();
            }
        }
        return kid;
    }

    @Override
    public int getKeyIdx(final int id, final int key) {
        final int[] keys = getKeys(id);
        return IntegerSets.binarySearch(keys, key, 0, keys.length);
    }

    @Override
    public int getKeyByIdx(final int id, final int idx) {
        return getKeys(id)[idx];
    }

    @Override
    public Object getValueByIdx(final int id, final int idx) {
        return getValues(id)[idx];
    }

    @Override
    public Object getValue(final int id, final int key) {
        final int idx = getKeyIdx(id, key);
        return idx > -1 ? getValueByIdx(id, idx) : null;
    }

    @Override
    public Keyword getKeywordByIdx(final int id, final int idx) {
        return (Keyword) getValue(getKeyByIdx(id, idx), KEY_ID);
    }

    @Override
    public boolean containsKey(final int id, final int key) {
        return getKeyIdx(id, key) > -1;
    }

    @Override
    public Keyword getKeyword(final int id) {
        return (Keyword) getValue(id, KEY_ID);
    }

    @Override
    public Keyword[] getKeysAsKeywords(final int id) {
        final int[] keys = getKeys(id);
        final Keyword[] result = new Keyword[keys.length];
        for (int i=0; i < keys.length; i++) result[i] = getKeyword(keys[i]);
        return result;
    }

    @Override
    public DBMap get(int id) {
        return new DBMap(this, id);
    }

    @Override
    public Object[][] project(int[] keys, int[] ids) {
        final Object[][] result = new Object[ids.length][];
        Object[] tmp;
        for (int i=0; i < ids.length; i++) {
            tmp = new Object[keys.length];
            for (int j=0; j < keys.length; j++) tmp[j] = getValue(ids[i], keys[j]);
            result[i] = tmp;
        }
        return result;
    }

    @Override
    public KeyFrequencyPair[] getKeysByFrequency(int[] ids) { throw new UnsupportedOperationException("not yet"); }

    @Override
    public KeyFrequencyPair[] getKeysByFrequency(int[] ids, int[] skipKeys) { throw new UnsupportedOperationException("not yet"); }

    @Override
    public KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey) { throw new UnsupportedOperationException("not yet"); }

    @Override
    public KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey, int[] skipKeys) { throw new UnsupportedOperationException("not yet"); }

        /* persistent fns */

    @Override
    public WritableDB insert(final int[] ks, final Object[] vs) {
        return insert(null, ks, vs);
    }

    @Override
    public WritableDB insert(final IndexAggregator aggregator, final int[] ks, final Object[] vs) {
        final int id = maxId + 1;
        final int[] ks1 = new int[ks.length + 1];
        ks1[0] = 0;
        System.arraycopy(ks, 0, ks1, 1, ks.length);
        final Object[] vs1 = new Object[ks1.length];
        vs1[0] = id;
        System.arraycopy(vs, 0, vs1, 1, ks.length);
        if (aggregator != null) {
            for (int key: ks1) {
                aggregator.add(key, id);
            }
        }
        IPersistentMap updatedUniqueIndices = updateIndices(id, ks, vs);
        // vs[0] has to be the keyword by necessity... could check.
        return new PersistentDB(identity,
                                updatedUniqueIndices,
                                keyIndex.assoc(id, ks1),
                                valIndex.assoc(id, vs1),
                                id);
    }

    @Override
    public WritableDB update(final int id, final int key, final Object val) {
        return update(null, id, key, val);
    }

    @Override
    public WritableDB update(final IndexAggregator aggregator, final int id, final int key, final Object val) {
        final int idx = getKeyIdx(id, key);
        final Object[] vs = getValues(id);
        if (idx > 0) { // should not change id
            final Object[] vs1 = new Object[vs.length];
            System.arraycopy(vs, 0, vs1, 0, vs.length);
            vs1[idx] = val;
            IPersistentMap updatedUniqueIndices = updateIndices(id, new int[] { key }, new Object[] { val });
            return new PersistentDB(identity,
                                    updatedUniqueIndices,
                                    keyIndex,
                                    valIndex.assoc(id, vs1),
                                    maxId);
        } else if (idx < 0) {
            final int[] ks = getKeys(id);
            final int idx2 = IntegerSets.binarySearchGreater(ks, key);
            final int[] ks1 = new int[ks.length + 1];
            final Object[] vs1 = new Object[vs.length + 1];
            if (idx2 == ks.length) { // update end
                System.arraycopy(ks, 0, ks1, 0, ks.length);
                ks1[ks.length] = key;
                System.arraycopy(vs, 0, vs1, 0, vs.length);
                vs1[vs.length] = val;
            } else { // update mid
                System.arraycopy(ks, 0, ks1, 0, idx2);
                ks1[idx2] = key;
                System.arraycopy(ks, idx2, ks1, (idx2 + 1), (ks.length - idx2));
                System.arraycopy(vs, 0, vs1, 0, idx2);
                vs1[idx2] = val;
                System.arraycopy(vs, idx2, vs1, (idx2 + 1), (vs.length - idx2));

            }
            if (aggregator != null) {
                aggregator.add(key, id);
            }
            IPersistentMap updatedUniqueIndices = updateIndices(id, new int[] { key }, new Object[] { val });
            return new PersistentDB(identity,
                                    updatedUniqueIndices,
                                    keyIndex.assoc(id, ks1),
                                    valIndex.assoc(id, vs1),
                                    maxId);
        } else {
            throw new RuntimeException("update0 - Could not update " +
                    "(id: " + id + ", key: " + key + ", idx: " + idx + ")");
        }
    }

    void printKeys(String label, int[] keys) {
        System.out.print(label + ": ");
        for (int i=0; i < keys.length; i++) {
            System.out.print(keys[i] + " ");
        }
        System.out.println();
    }

    void printVals(String label, Object[] vals) {
        System.out.print(label + ": ");
        for (int i=0; i < vals.length; i++) {
            System.out.print(vals[i] + " ");
        }
        System.out.println();
    }

    @Override
    public WritableDB update(final IndexAggregator aggregator, final int id,
                             final int[] keys, final Object[] vals) {
        final int[] prevKeys = getKeys(id);
        final Object[] prevVals = getValues(id);
        final int[] ks = IntegerSets.union(prevKeys, keys);
        Keyword dbKey = null;
        final Object[] vs = new Object[ks.length];
        for (int i=0; i < ks.length; i++) {
            int idx = IntegerSets.binarySearch(keys, ks[i], 0, keys.length);
            if (idx >= 0 && idx < keys.length) {
                 vs[i] = vals[idx];
                 if (aggregator != null) {
                     aggregator.add(ks[i], id);
                 }
            } else {
                idx = IntegerSets.binarySearch(prevKeys, ks[i], 0, prevKeys.length);
                vs[i] = prevVals[idx];
            }
        }
        int idx = IntegerSets.binarySearch(keys, KEY_ID, 0, keys.length);
        if (idx >= 0) {
            dbKey = (Keyword) vals[idx];
        }
        IPersistentMap updatedUniqueIndices = updateIndices(id, keys, vals);
        return new PersistentDB(identity,
                                updatedUniqueIndices,
                                keyIndex.assoc(id, ks),
                                valIndex.assoc(id, vs),
                                maxId);
    }

    @Override
    public WritableDB replace(final IndexAggregator aggregator, final int id,
                              final int[] keys, final Object[] vals) {
        final int[] prevKeys = getKeys(id);

        // remove id from dropped key's db/ids
        final int[] removedKeys = IntegerSets.difference(prevKeys, keys);
        for (int i=0; i < removedKeys.length; i++) {
            aggregator.remove(removedKeys[i], id);
        }
        // add new keys to index aggr for id
        final int[] newKeys = IntegerSets.difference(keys, prevKeys);
        for (int i=0; i < newKeys.length; i++) {
            aggregator.add(newKeys[i], id);
        }

        Keyword dbKey = null;
        int idx = IntegerSets.binarySearch(keys, KEY_ID, 0, keys.length);
        if (idx >= 0) {
            dbKey = (Keyword) vals[idx];
        }
        IPersistentMap replacedKeyIndices = replaceIndices(id, keys, vals, removedKeys);
        return new PersistentDB(identity,
                                replacedKeyIndices,
                                keyIndex.assoc(id, keys),
                                valIndex.assoc(id, vals),
                                maxId);
    }

    @Override
    public WritableDB updateInline(final IndexAggregator aggregator,
                                   final int id,
                                   final int[] keys,
                                   final Object[] vals) {
        final int idx = getKeyIdx(id, keys[0]);
        final Object[] vs = getValues(id);
        if (idx < 0) {
            final int[] ks = getKeys(id);
            final int idx2 = IntegerSets.binarySearchGreater(ks, keys[0]);
            final int[] ks1 = new int[ks.length + keys.length];
            final Object[] vs1 = new Object[vs.length + vals.length];
            if (idx2 == ks.length) { // update end
                System.arraycopy(ks, 0, ks1, 0, ks.length);
                System.arraycopy(keys, 0, ks1, idx2, keys.length);
                System.arraycopy(vs, 0, vs1, 0, vs.length);
                System.arraycopy(vals, 0, vs1, idx2, vals.length);
            } else { // update mid
                System.arraycopy(ks, 0, ks1, 0, idx2);
                System.arraycopy(keys, 0, ks1, idx2, keys.length);
                System.arraycopy(ks, idx2, ks1, (idx2 + keys.length), (ks.length - idx2));
                System.arraycopy(vs, 0, vs1, 0, idx2);
                System.arraycopy(vals, 0, vs1, idx2, vals.length);
                System.arraycopy(vs, idx2, vs1, (idx2 + vals.length), (vs.length - idx2));

            }
            if (aggregator != null) {
                for (int key: keys) {
                    aggregator.add(key, id);
                }
            }
            IPersistentMap updatedUniqueIndices = updateIndices(id, keys, vals);
            return new PersistentDB(identity,
                                    uniqueIndices,
                                    keyIndex.assoc(id, ks1),
                                    valIndex.assoc(id, vs1),
                                    maxId);
        } else {
            throw new RuntimeException("updateInline - Could not updateInline " +
                    "(id: " + id + ", keys: " + keys + ", idx: " + idx + ")");
        }
    }

    private boolean[] keysUnique(final int[] keys) {
        boolean[] result = new boolean[keys.length];
        for (int i=0; i < keys.length; i++) {
            Object value = getValue(keys[i], DB.UNIQUE_TAG_ID);
            result[i] = (value != null && value instanceof Boolean && true == ((Boolean) value));
        }
        return result;
    }

    private IPersistentMap updateIndices(final int id, final int[] keys, final Object[] vals) {
        return IndexAggregator.updateIndices(uniqueIndices, id, keys, keysUnique(keys), vals);
    }

    private IPersistentMap replaceIndices(final int id, final int[] keys, final Object[] vals, final int[] keysToRemove) {
        IPersistentMap result = IndexAggregator.removeFromIndices(uniqueIndices, keysToRemove, keysUnique(keysToRemove), vals);
        return IndexAggregator.updateIndices(result, id, keys, keysUnique(keys), vals);
    }

    public DB compactToRDB() {
        RDB.C[] cs = new RDB.C[maxId + 1];
        for (int i=0; i <= maxId; i++) {
            cs[i] = new RDB.C((int[]) keyIndex.valAt(i, null), (Object[]) valIndex.valAt(i, null));
        }
        return new RDB(identity, uniqueIndices, cs, maxId, new IntArrayPool());
    }

    @Override
    public void shutdown() {}
}
