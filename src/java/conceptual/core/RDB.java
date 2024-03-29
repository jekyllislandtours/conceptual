package conceptual.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import conceptual.util.IntArrayPool;
import conceptual.util.IntegerSets;
import conceptual.util.ZipTools;

import java.io.*;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

import javax.crypto.*;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public final class RDB implements DB, WritableDB {

    public final Keyword identity;

    public final IPersistentMap uniqueIndices;
    // TODO add int[][] view into keys for freq stuff.
    public final C[] cs;
    public final int maxId;

    public final IntArrayPool intArrayPool;

    public final static Set<Keyword> unknownKeywords = new HashSet<Keyword>();

    public RDB(final Keyword identity,
               final IPersistentMap uniqueIndices,
               final C[] cs,
               final int maxId,
               final IntArrayPool intArrayPool) {
        this.identity = identity;
        this.uniqueIndices = uniqueIndices;
        this.cs = cs;
        this.maxId = maxId;
        this.intArrayPool = intArrayPool;
    }

    public final static class C {
        final int[] ks;
        final Object[] vs;

        public C(final int[] ks, final Object[] vs) {
            this.ks = ks;
            this.vs = vs;
        }
    }

    @Override
    public Keyword getIdentity() {
        return identity;
    }

    @Override
    public int getMaxId() {
        return maxId;
    }

    @Override
    public int count() { return maxId + 1; }

    @Override
    public int getTripleCount() {
        int result = 0;
        C tmp;
        for (int i=0; i < count(); i++) {
            tmp = cs[i];
            for (int j=0; j < tmp.vs.length; j++) {
                if (tmp.vs[j] != null && tmp.vs[j] instanceof int[]) {
                    result += ((int[]) tmp.vs[j]).length;
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
        for (int i=0; i < count(); i++) {
            result += cs[i].ks.length;
        }
        return result;
    }

    @Override
    public Integer lookupId(int uniqueKey, Object key) {
        Integer result = null;
        IPersistentMap keyIndex = (IPersistentMap) uniqueIndices.valAt(uniqueKey);
        if (keyIndex != null) {
            result = (Integer) keyIndex.valAt(key);
        }
        return result;
    }

    @Override
    public Integer keywordToId(Keyword key) {
        return lookupId(DB.KEY_ID, key);
    }

    @Override
    public int keyToId(Object key) {
        int kid = -1;
        if (key != null) {
            if (key instanceof Keyword) {
                final Keyword kw = (Keyword) key;
                Object id = keywordToId(kw);
                if (id != null) {
                    kid = ((Integer) id);
                } else if(!unknownKeywords.contains(kw)) {
                    unknownKeywords.add(kw);
                    System.err.println("WARN: conceptual.core.RDB id not found for keyword: " + kw);
                }
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
    public int[] getKeys(final int id) {
        return cs[id].ks;
    }

    @Override
    public Object[] getValues(final int id) {
        return cs[id].vs;
    }

    @Override
    public int getKeyIdx(final int id, final int key) {
        final int[] keys = cs[id].ks;
        return IntegerSets.binarySearch(keys, key, 0, keys.length);
    }

    @Override
    public int getKeyByIdx(final int id, final int idx) {
        return cs[id].ks[idx];
    }

    @Override
    public Object getValueByIdx(final int id, final int idx) {
        return idx > -1 ? cs[id].vs[idx] : null;
    }

    @Override
    public Object getValue(final int id, final int key) {
        final C c = cs[id];
        final int[] keys = c.ks;
        final int idx = IntegerSets.binarySearch(keys, key, 0, keys.length);
        return idx > -1 ? c.vs[idx] : null;
    }

    @Override
    public Keyword getKeywordByIdx(final int id, final int idx) {
        Keyword result = null;
        if (idx > -1) {
            result = (Keyword) getValue(cs[id].ks[idx], KEY_ID);
        }
        return result;
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
    public DBMap lookup(int uniqueKey, Object key) {
        Integer id = lookupId(uniqueKey, key);
        DBMap result = null;
        if (id != null) {
            result = new DBMap(this, id);
        }
        return result;
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
    public KeyFrequencyPair[] getKeysByFrequency(int[] ids) {
        if (ids == null) return null;
        int[] freqBins = null;
        try {
            try {
                freqBins = intArrayPool.borrowArray(count());
            } catch (Exception e) {
                freqBins = new int[maxId + 1];
            }

            int usedBins = 0;
            int[] keys;

            for (int id: ids) {
                keys = cs[id].ks;
                for (int key: keys) {
                    if (freqBins[key] == 0) usedBins++;
                    freqBins[key]++;
                }
            }

            KeyFrequencyPair[] result = new KeyFrequencyPair[usedBins];
            int idx = 0;
            for (int i=0; i < freqBins.length; i++) {
                if (freqBins[i] > 0) {
                    result[idx] = new KeyFrequencyPair(i, freqBins[i]);
                    idx++;
                }
            }

            Arrays.sort(result, KeyFrequencyPair.KeyFrequencyPairComparator);

            return result;
        } finally {
            if (freqBins != null) {
                try {
                    intArrayPool.returnArray(freqBins);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public KeyFrequencyPair[] getKeysByFrequency(int[] ids, int[] skipKeys) {
        if (ids == null) return null;
        int[] freqBins = null;
        try {
            try {
                freqBins = intArrayPool.borrowArray(count());
            } catch (Exception e) {
                freqBins = new int[maxId + 1];
            }

            int usedBins = 0;
            int[] keys;

            for (int id: ids) {
                keys = cs[id].ks;
                for (int key: keys) {
                    if (freqBins[key] == 0) usedBins++;
                    freqBins[key]++;
                }
            }

            int idx = 0;
            KeyFrequencyPair[] unfiltered = new KeyFrequencyPair[usedBins];
            for (int i=0; i < freqBins.length; i++) {
                if (freqBins[i] > 0) {
                    unfiltered[idx] = new KeyFrequencyPair(i, freqBins[i]);
                    idx++;
                }
            }

            int filtered = 0;
            if (skipKeys != null && skipKeys.length > 0) {
                int filterIndex = 0;
                for (KeyFrequencyPair uf: unfiltered) {
                    if (filterIndex < skipKeys.length) {
                        while (filterIndex < skipKeys.length &&
                                skipKeys[filterIndex] < uf.key) {
                            filterIndex++;
                        }
                        if (filterIndex < skipKeys.length &&
                                skipKeys[filterIndex] == uf.key) {
                            filterIndex++;
                            uf.frequency = -1;
                            filtered++;
                        }
                    }
                }
            }

            Arrays.sort(unfiltered, KeyFrequencyPair.KeyFrequencyPairComparator);

            KeyFrequencyPair[] result = new KeyFrequencyPair[unfiltered.length - filtered];
            System.arraycopy(unfiltered, 0, result, 0, result.length);
            return result;
        } finally {
            if (freqBins != null) {
                try {
                    intArrayPool.returnArray(freqBins);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey) {
        if (ids == null) return null;

        int[] freqBins = null;
        try {
            try {
                freqBins = intArrayPool.borrowArray(count());
            } catch (Exception e) {
                freqBins = new int[maxId + 1];
            }

            boolean toManyRelation = getValue(relationKey, DB.TO_MANY_RELATION_TAG_ID) != null ? true : false;
            boolean toOneRelation = getValue(relationKey, DB.TO_ONE_RELATION_TAG_ID) != null ? true : false;

            Object temp;
            int usedBins = 0;
            if (toManyRelation) {
                int[] relations;
                for (int id : ids) {
                    temp = getValue(id, relationKey);
                    if (temp != null) {
                        relations = (int[]) temp;
                        for (int rel : relations) {
                            if (freqBins[rel] == 0) usedBins++;
                            freqBins[rel]++;
                        }
                    }
                }
            } else if (toOneRelation) {
                int relation;
                for (int id : ids) {
                    temp = getValue(id, relationKey);
                    if (temp != null) {
                        relation = (int) temp;
                        if (freqBins[relation] == 0) usedBins++;
                        freqBins[relation]++;
                    }
                }
            }

            KeyFrequencyPair[] result = new KeyFrequencyPair[usedBins];
            int idx = 0;
            for (int i=0; i < freqBins.length; i++) {
                if (freqBins[i] > 0) {
                    result[idx] = new KeyFrequencyPair(i, freqBins[i]);
                    idx++;
                }
            }

            Arrays.sort(result, KeyFrequencyPair.KeyFrequencyPairComparator);

            return result;
        } finally {
            try {
                intArrayPool.returnArray(freqBins);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public KeyFrequencyPair[] getRelationsByFrequency(int[] ids, int relationKey, int[] skipKeys) {
        if (ids == null) return null;
        int[] freqBins = null;
        try {
            int[] relations;
            try {
                freqBins = intArrayPool.borrowArray(count());
            } catch (Exception e) {
                freqBins = new int[maxId + 1];
            }

            Object temp;
            int usedBins = 0;
            for (int id: ids) {
                temp = getValue(id, relationKey);
                if (temp != null) {
                    relations = (int[]) temp;
                    for (int rel: relations) {
                        if (freqBins[rel] == 0) usedBins++;
                        freqBins[rel]++;
                    }
                }
            }

            int idx = 0;
            KeyFrequencyPair[] unfiltered = new KeyFrequencyPair[usedBins];
            for (int i=0; i < freqBins.length; i++) {
                if (freqBins[i] > 0) {
                    unfiltered[idx] = new KeyFrequencyPair(i, freqBins[i]);
                    idx++;
                }
            }

            int filtered = 0;
            if (skipKeys != null && skipKeys.length > 0) {
                int filterIndex = 0;
                for (KeyFrequencyPair uf: unfiltered) {
                    if (filterIndex < skipKeys.length) {
                        while (filterIndex < skipKeys.length &&
                                skipKeys[filterIndex] < uf.key) {
                            filterIndex++;
                        }
                        if (filterIndex < skipKeys.length &&
                                skipKeys[filterIndex] == uf.key) {
                            filterIndex++;
                            uf.frequency = -1;
                            filtered++;
                        }
                    }
                }
            }

            Arrays.sort(unfiltered, KeyFrequencyPair.KeyFrequencyPairComparator);

            KeyFrequencyPair[] result = new KeyFrequencyPair[unfiltered.length - filtered];
            System.arraycopy(unfiltered, 0, result, 0, result.length);

            return result;
        } finally {
            try {
                intArrayPool.returnArray(freqBins);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    private boolean hasCapacity(int minCapacity) {
        return (minCapacity - cs.length < 0);
    }

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
            for (int k: ks1) {
                aggregator.add(k, id);
            }
        }

        C[] newCS;
        if (!hasCapacity(id)) {
            int oldCapacity = cs.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity - id < 0)
                newCapacity = id;
            if (newCapacity - MAX_ARRAY_SIZE > 0)
                newCapacity = hugeCapacity(id);
            // minCapacity is usually close to size, so this is a win:
            newCS = Arrays.copyOf(cs, newCapacity);
        } else {
            newCS = cs;
        }

        newCS[id] = new C(ks1, vs1);
        IPersistentMap updatedUniqueIndices = updateIndices(id, ks, vs);

        // vs[0] has to be the keyword by necessity... could check.
        return new RDB(identity,
                       updatedUniqueIndices,
                       newCS,
                       id,
                       intArrayPool);
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
            cs[id] = new C(cs[id].ks, vs1);
            IPersistentMap updatedUniqueIndices = updateIndices(id, new int[] { key }, new Object[] { val });
            return new RDB(identity,
                           updatedUniqueIndices,
                           cs,
                           maxId,
                           intArrayPool);
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
            cs[id] = new C(ks1, vs1);
            IPersistentMap updatedUniqueIndices = updateIndices(id, new int[] { key }, new Object[] { val });
            return new RDB(identity,
                           updatedUniqueIndices,
                           cs,
                           maxId,
                           intArrayPool);
        } else {
            throw new RuntimeException("update0 - Could not update " +
                    "(id: " + id + ", key: " + key + ", idx: " + idx + ")");
        }
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
        cs[id] = new C(ks, vs);
        IPersistentMap updatedUniqueIndices = updateIndices(id, ks, vs);
        return new RDB(identity,
                       updatedUniqueIndices,
                       cs,
                       maxId,
                       intArrayPool);
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
        cs[id] = new C(keys, vals);
        IPersistentMap replacedKeyIndices = replaceIndices(id, keys, vals, removedKeys);
        return new RDB(identity,
                       replacedKeyIndices,
                       cs,
                       maxId,
                       intArrayPool);
    }

    @Override
    public WritableDB updateInline(final IndexAggregator aggregator, int id, final int[] ks, final Object[] vs) {
        throw new UnsupportedOperationException();
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


    public static DB load(final String filename)
        throws IOException
    {
        return load(filename, false);
    }

    public static DB load(final String filename, final boolean verbose)
            throws IOException
    {
        try (FileInputStream fis = new FileInputStream(filename);
             BufferedInputStream bis = new BufferedInputStream(fis);
             InputStream zis = ZipTools.getCompressedInputStream(bis, filename);
             DataInputStream dis = new DataInputStream(zis)) {
            return DBTranscoder.decodeRDB(dis, verbose);
        }
    }

    public static DB load(final String filename, final boolean verbose, final Cipher cipher)
        throws IOException
    {
        try (FileInputStream fis = new FileInputStream(filename);
             BufferedInputStream bis = new BufferedInputStream(fis);
             CipherInputStream cis = new CipherInputStream(bis, cipher);
             InputStream zis = ZipTools.getCompressedInputStream(cis, filename);
             DataInputStream dis = new DataInputStream(zis)) {
            return DBTranscoder.decodeRDB(dis, verbose);
        }
    }

    public static void store(final RDB db, final String filename)
            throws IOException
    {
        try (FileOutputStream fos = new FileOutputStream(filename, false);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             OutputStream zos = ZipTools.getCompressedOutputStream(bos, filename);
             DataOutputStream dos = new DataOutputStream(zos)) {
            DBTranscoder.encode(dos, db);
            dos.flush();
            bos.flush();
            zos.flush();
            fos.flush();
        }
    }

    public static void store(final RDB db, final String filename, final Cipher cipher)
        throws IOException
    {
        try (FileOutputStream fos = new FileOutputStream(filename, false);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             CipherOutputStream cos = new CipherOutputStream(bos, cipher);
             OutputStream zos = ZipTools.getCompressedOutputStream(cos, filename);
             DataOutputStream dos = new DataOutputStream(zos)) {
            DBTranscoder.encode(dos, db);
            cos.flush();
            dos.flush();
            bos.flush();
            zos.flush();
            fos.flush();
        }
    }

    @Override
    public void shutdown() {}
}
