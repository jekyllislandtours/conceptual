package conceptual.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.io.Utils;

import conceptual.util.Bytes;
import conceptual.util.IntArrayPool;
import conceptual.util.IntegerSets;
import conceptual.util.ZipTools;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public final class TuplDB implements WritableDB {

    final Keyword identity;
    final Index keyIdIndex;
    final Index keyIndex;
    final Index eav;

    public TuplDB(Keyword identity) throws IOException {
        this.identity = identity;
        this.keyIdIndex = TuplInterop.getIndex(identity, Keyword.intern(identity.sym.toString(), "key-id-idx"));
        this.keyIndex = TuplInterop.getIndex(identity, Keyword.intern(identity.sym.toString(), "key-idx"));
        this.eav = TuplInterop.getIndex(identity, Keyword.intern(identity.sym.toString(), "eav"));
    }

    public Database getDatabase() {
        return TuplInterop.getDatabase(identity);
    }

    @Override
    public Keyword getIdentity() { return identity; }

    @Override
    public int getMaxId() {
        int result = -1;
        Cursor c = keyIndex.newCursor(Transaction.BOGUS);
        try {
            c.last();
            byte[] keyBytes = c.key();
            if (keyBytes != null) {
                result = (Integer) Utils.decodeIntBE(keyBytes, 0);
            }
        } catch (Exception e) {
            //throw new RuntimeException(e);
        } finally {
            c.reset();
        }
        return result;
    }

    @Override
    public int count() {
        return getMaxId() + 1;
    }

    @Override
    public int getTripleCount() {
        int result = 0;
        int maxId = getMaxId();
        for (int i=0; i <= maxId; i++) {
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
        int maxId = getMaxId();
        for (int i=0; i <= maxId; i++) {
            result += getKeys(i).length;
        }
        return result;
    }

    @Override
    public Integer keywordToId(final Keyword key) {
        if (key == null) {
            return null;
        }
        else {
            try {
                return keywordToId(key.sym.toString().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Integer keywordToId(final byte[] key) {
        Integer result = null;
        try {
            byte[] bytes = keyIdIndex.load(Transaction.BOGUS, key);
            if (bytes != null) {
                result = Utils.decodeIntBE(bytes, 0);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    public int[] getKeys(final int id) {
        byte[] idb = new byte[4];
        Utils.encodeIntBE(idb, 0, id);
        return getKeys(idb);
    }

    public int[] getKeys(final byte[] id) {
        try {
            byte[] keyBytes = keyIndex.load(null, id);
            return DBTranscoder.decodeKeys(keyBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object[] getValues(final int id) {
        return getValues(id, getKeys(id));
    }

    // TODO
    public Object[] getValues(final int id, final int[] keys) {
        Object[] vals = null;
        if (keys != null) {
            vals = new Object[keys.length];

            byte[] start = new byte[8];
            byte[] end = new byte[8];
            Bytes.encodeIntPairBE(start, 0, id, keys[0]);
            Bytes.encodeIntPairBE(end, 0, id, keys[keys.length - 1]);

            Cursor pull = this.eav.newCursor(Transaction.BOGUS);
            try {
                pull.findGe(start);
                int i = 0;
                while (pull.key() != null) {
                    vals[i++] = DBTranscoder.decodeVal(pull.value());
                    pull.nextLe(end);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                pull.reset();
            }
        }
        return vals;
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
        Object result = null;
        byte[] pair = new byte[8];
        Bytes.encodeIntPairBE(pair, 0, id, key);
        try {
            byte[] v = eav.load(null, pair);
            if (v != null) {
                result = DBTranscoder.decodeVal(v);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public Keyword getKeywordByIdx(final int id, final int idx) {
        return (Keyword) getValue(getKeyByIdx(id, idx), KEY_ID);
    }

    @Override
    public boolean containsKey(final int id, final int key) {
        return (getValue(id, key) != null);
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
        return new DBTuplMap(this, id);
    }

    // TODO could speed up using cursors
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
        final int maxId = getMaxId();
        final int id = (maxId == -1) ? 0: maxId + 1;
        final int[] ks1 = new int[ks.length + 1];
        ks1[ID_ID] = ID_ID;
        System.arraycopy(ks, 0, ks1, 1, ks.length);
        final Object[] vs1 = new Object[ks1.length];
        vs1[ID_ID] = id;
        System.arraycopy(vs, 0, vs1, 1, ks.length);
        Keyword key = null;
        if (ks[0] == KEY_ID) {
            key = (Keyword) vs[0];
        }

        if (keywordToId(key) == null) {
            try {
                byte[] idb = new byte[4];
                Utils.encodeIntBE(idb, 0, id);
                if (key != null) {
                    keyIdIndex.store(Transaction.BOGUS, key.sym.toString().getBytes("UTF-8"), idb);
                }
                keyIndex.store(Transaction.BOGUS, idb, DBTranscoder.encodeKeys(ks1));
                for (int i=0; i < ks1.length; i++) {
                    byte[] eav_key = new byte[8];
                    Bytes.encodeIntPairBE(eav_key, 0, id, ks1[i]);
                    eav.store(Transaction.BOGUS, eav_key, DBTranscoder.encodeVal(vs1[i]));
                    if (aggregator != null) {
                        aggregator.add(ks1[i], id);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("insert failed. key already exists: " + key);
        }
        return this;
    }

    @Override
    public WritableDB update(final int id, final int key, final Object val) {
        return update(null, id, key, val);
    }

    @Override
    public WritableDB update(final IndexAggregator aggregator, final int id, final int key, final Object val) {
        final int idx = getKeyIdx(id, key);
        final Object[] vs = getValues(id);
        try {
            byte[] idb = new byte[4];
            Utils.encodeIntBE(idb, 0, id);
            byte[] idKeyPair = new byte[8];
            Bytes.encodeIntPairBE(idKeyPair, 0, id, key);
            if (idx > 0) { // should not change id
                final Object[] vs1 = new Object[vs.length];
                System.arraycopy(vs, 0, vs1, 0, vs.length);
                vs1[idx] = val;
                if (key == KEY_ID) {
                    keyIdIndex.delete(Transaction.BOGUS, ((Keyword) vs[idx]).sym.toString().getBytes("UTF-8"));
                    keyIdIndex.store(Transaction.BOGUS, ((Keyword) val).sym.toString().getBytes("UTF-8"), idb);
                }
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
                keyIndex.replace(Transaction.BOGUS, idb, DBTranscoder.encodeKeys(ks1));
                if (aggregator != null) {
                    aggregator.add(key, id);
                }
            } else {
                throw new RuntimeException("update - Could not update " +
                        "(id: " + id + ", key: " + key + ", idx: " + idx + ")");
            }
            eav.store(Transaction.BOGUS, idKeyPair, DBTranscoder.encodeVal(val));
        } catch (IOException e) {
            throw new RuntimeException("update - Could not update " +
                    "(id: " + id + ", key: " + key + ", val: " + val + ")", e);
        }
        return this;
    }

    @Override
    public WritableDB update(final IndexAggregator aggregator, final int id,
                             final int[] keys, final Object[] vals) {
        // TODO: fix
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableDB updateInline(final IndexAggregator aggregator, int id, final int[] ks, final Object[] vs) {
        throw new UnsupportedOperationException();
    }

    private IPersistentMap extractKeyIdIdx() {
        Map<Keyword, Integer> temp = new HashMap<>();
        Cursor c = keyIdIndex.newCursor(null);
        try {
            c.first();
            byte[] keyBytes;
            while ((keyBytes = c.key()) != null) {
                Keyword key = Keyword.intern(new String(keyBytes, "UTF-8"));
                Integer val = Utils.decodeIntBE(c.value(), 0);
                temp.put(key, val);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            c.reset();
        }
        return PersistentHashMap.create(temp);
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
             InputStream is = ZipTools.getCompressedInputStream(fis, filename);
             BufferedInputStream bis = new BufferedInputStream(is, 0x100_000);
             DataInputStream dis = new DataInputStream(bis)) {
            RDB db = DBTranscoder.decodeRDB(dis, false);
            return TuplDB.clone(db);
        }
    }

    public void store(final String filename)
            throws IOException
    {
        RDB db = (RDB) compactToRDB();
        try (FileOutputStream fos = new FileOutputStream(filename, false);
             OutputStream os = ZipTools.getCompressedOutputStream(fos, filename);
             DataOutputStream dos = new DataOutputStream(os)) {
            DBTranscoder.encode(dos, db);
            dos.flush();
            os.flush();
            fos.flush();
        }
    }

    public DB compactToRDB() {
        int maxId = getMaxId();
        RDB.C[] cs = new RDB.C[maxId + 1];
        for (int i=0; i <= maxId; i++) {
            cs[i] = new RDB.C(getKeys(i), getValues(i));
        }
        return new RDB(identity, extractKeyIdIdx(), cs, maxId, new IntArrayPool());
    }

    private static void cloneKeyIdIndex(TuplDB tuplDB, DB db) throws IOException {
        System.out.println("cloning key-id index");
        int maxId = db.getMaxId();
        byte[] idb = new byte[4];
        byte[] eav_key = new byte[8];
        Cursor fill = tuplDB.keyIdIndex.newCursor(Transaction.BOGUS);
        try {
            double last = 0.0;
            double current;
            double lengthD = (double) maxId;
            for (int i = 0; i <= maxId; i++) {
                Keyword key = (Keyword) db.getValue(i, KEY_ID);
                if (key != null) {
                    Utils.encodeIntBE(idb, 0, i);
                    fill.findNearby(key.sym.toString().getBytes("UTF-8"));
                    fill.store(idb);
                }
                current = Math.round(((double) i/ lengthD) * 100.0d);
                if (current != last) {
                    last = current;
                    System.out.printf("\r    %3.0f%% complete.", current);
                }
            }
            System.out.println();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fill.reset();
        }
        tuplDB.getDatabase().checkpoint();
    }

    private static void cloneKeyIndex(TuplDB tuplDB, DB db) throws IOException {
        System.out.println("cloning key index");
        int maxId = db.getMaxId();
        byte[] idb = new byte[4];
        Cursor fill = tuplDB.keyIndex.newCursor(Transaction.BOGUS);
        try {
            double last = 0.0;
            double current;
            double lengthD = (double) maxId;
            for (int i = 0; i <= maxId; i++) {
                Utils.encodeIntBE(idb, 0, i);
                fill.findNearby(idb);
                fill.store(DBTranscoder.encodeKeys(db.getKeys(i)));
                current = Math.round(((double) i/ lengthD) * 100.0d);
                if (current != last) {
                    last = current;
                    System.out.printf("\r    %3.0f%% complete.", current);
                }
            }
            System.out.println();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fill.reset();
        }
        tuplDB.getDatabase().checkpoint();
    }

    private static void cloneValueIndex(TuplDB tuplDB, DB db) throws IOException {
        System.out.println("cloning value index");
        int maxId = db.getMaxId();
        byte[] key = new byte[8];
        int[] ks;
        Object[] vs;
        Cursor fill = tuplDB.eav.newCursor(Transaction.BOGUS);
        try {
            double last = 0.0;
            double current;
            double lengthD = (double) maxId;
            for (int i = 0; i <= maxId; i++) {
                ks = db.getKeys(i);
                vs = db.getValues(i);
                for (int j = 0; j < ks.length; j++) {
                    Bytes.encodeIntPairBE(key, 0, i, ks[j]);
                    fill.findNearby(key);
                    fill.store(DBTranscoder.encodeVal(vs[j]));
                    current = Math.round(((double) i/ lengthD) * 100.0d);
                    if (current != last) {
                        last = current;
                        System.out.printf("\r    %3.0f%% complete.", current);
                    }
                }
            }
            System.out.println();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fill.reset();
        }
        tuplDB.getDatabase().checkpoint();
    }

    public static TuplDB clone(DB db) {
        TuplDB result;
        try {
            result = new TuplDB(db.getIdentity());
            cloneKeyIdIndex(result, db);
            cloneKeyIndex(result, db);
            cloneValueIndex(result, db);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }


    // TODO handle RDB case

    @Override
    public void shutdown() {}
}
