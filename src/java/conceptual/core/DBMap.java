package conceptual.core;

import clojure.lang.AFn;
import clojure.lang.Associative;
import clojure.lang.Counted;
import clojure.lang.ILookup;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Sequential;

import java.io.IOException;
import java.io.StringWriter;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a lazy implementation of a map facade on the idea of a concept.
 */
public class DBMap extends AFn implements ILookup, IPersistentMap, Map, Iterable, Associative, Counted {

    protected final int id;
    protected final DB db;

    public DBMap(DB db, int id) {
        this.db = db;
        this.id = id;
    }

    /* DBMap specific properties */

    public int getId() {
        return id;
    }

    public Keyword getKeyword() {
        return (Keyword) get(DB.KEY_ID);
    }

    public Object getType() {
        return get(DB.TYPE_ID);
    }

    public int[] getKeys() {
        return db.getKeys(id);
    }

    public Object[] getValues() {
        return db.getValues(id);
    }

    /* Map */

    /** Removes all of the mappings from this map (optional operation). */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("DBMap is read-only, so clear is not supported.");
    }

    /** Returns true if this map contains a mapping for the specified key. */
    @Override
    public boolean containsKey(Object key) {
        return db.containsKey(id, db.keyToId(key));
    }

    /** Returns true if this map maps one or more keys to the specified value. */
    @Override
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    /** Compares the specified object with this map for equality. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBMap dbMap = (DBMap) o;
        return id != dbMap.id && db.equals(dbMap.db);
    }

    /** Returns the hash code value for this map. */
    @Override
    public int hashCode() {
        return id;
    }

    /** Returns the value to which the specified key is mapped, or null if
     * this map contains no mapping for the key. */
    @Override
    public Object get(Object key) {
        int kid = db.keyToId(key);
        if (kid > -1) {
            return db.getValue(id, kid);
        } else return null;
    }

    /**
     * Returns the value for a given key.
     *
     * @param key the id of the key for the value to be returned.
     * @return Object the value for the given key.
     */
    public Object get(int key) {
        return db.getValue(id, key);
    }

    /**
     * Returns an array of values corresponding to an array of keys.
     * @param keys the keys whose values are to be projected.
     * @return the set of values corresponding to the keys.
     */
    public Object[] project(int[] keys) {
        Object[] result = new Object[keys.length];
        for (int i=0; i < keys.length; i++) {
            result[i] = get(keys[i]);
        }
        return result;
    }

    /**
     * Returns a ProjectedMap that presents a subspace "projection" of it's
     * original keyspace.
     * @param keys the projected set of keys.
     * @return the projected map.
     */
    public ProjectedMap projectMap(int[] keys) {
        return new ProjectedMap(db, id, keys);
    }

    /**
     * Returns a compolete projected map containing every key.
     * @return the projected map.
     */
    public ProjectedMap projectMap() {
        return new ProjectedMap(db, id);
    }

    /** Returns true if this map contains no key-value mappings. */
    @Override
    public boolean isEmpty() {
        return (size() == 0);
    }

    /** Returns a Set view of the keys contained in this map. */
    @Override
    public Set keySet() {
        return new LinkedHashSet<>(Arrays.asList(db.getKeysAsKeywords(id)));
    }

    /**
     * Returns this maps keys as an array of Keywords.
     * @return
     */
    public Keyword[] getKeysAsKeywords() {
        final int[] keys = getKeys();
        final Keyword[] result = new Keyword[keys.length];
        for (int i=0; i < keys.length; i++) result[i] = db.getKeyword(keys[i]);
        return result;
    }

    /** Associates the specified value with the specified key in this map (optional operation). */
    @Override
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException("DBMap is read-only, so put is not supported.");
    }

    /** Copies all of the mappings from the specified map to this map (optional operation). */
    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException("DBMap is read-only, so putAll is not supported.");
    }

    /** Removes the mapping for a key from this map if it is present (optional operation). */
    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("DBMap is read-only, so remove is not supported.");
    }

    /** Returns the number of key-value mappings in this map. */
    @Override
    public int size() {
        return db.getKeys(id).length;
    }

    /** Returns a Set view of the mappings contained in this map. */
    @Override
    public Set entrySet() {
        return new AbstractSet() {
            public Iterator iterator() {
                return DBMap.this.iterator();
            }

            public int size() {
                return DBMap.this.size();
            }

            public int hashCode() {
                return DBMap.this.hashCode();
            }

            public boolean contains(Object o){
                throw new UnsupportedOperationException();
            }
        };
    }

    /** Returns a Collection view of the values contained in this map. */
    @Override
    public Collection values() {
        return Arrays.asList(db.getValues(id));
    }

    /* Countable, IPersistentCollection */
    @Override
    public int count() {
        return size();
    }

    /* ILookup */
    @Override
    public Object valAt(Object key, Object notFound){
        Object result = get(key);
        if (result != null) {
            return result;
        } else {
            return notFound;
        }
    }

    @Override
    public Object valAt(Object key){
        return valAt(key, null);
    }

    /* Iterable */

    @Override
    public Iterator iterator() {
        return new LazyIterator();
    }

    /* IFn */

    @Override
    public Object invoke(Object arg1) {
        return valAt(arg1);
    }

    @Override
    public Object invoke(Object arg1, Object arg2) {
        return valAt(arg1, arg2);
    }

    /* ISeq */

    @Override
    public ISeq seq() {
        return IteratorSeq.create(new LazyIterator());
    }

    @Override
    public IPersistentCollection cons(Object o) {
        return asPersistentMap().cons(o);
    }

    @Override
    public IPersistentCollection empty() {
        return PersistentHashMap.create();
    }

    @Override
    public boolean equiv(Object o) {
        return equals(o);
    }

    @Override
    public String toString() {
        return RT.printString(this);
    }

    /* Associative */

    @Override
    public IMapEntry entryAt(Object key) {
        final int kid = db.keyToId(key);
        DBMapEntry entry = null;
        if (kid > -1) {
            final int idx = db.getKeyIdx(id, kid);
            if (idx > -1) {
                entry = new DBMapEntry(idx);
            }
        }
        return entry;
    }

    @Override
    public IPersistentMap assoc(Object key, Object val) {
        return asPersistentMap().assoc(key, val);
    }

    @Override
    public IPersistentMap assocEx(Object key, Object val) {
        return assoc(key, val);
    }

    @Override
    public IPersistentMap without(Object key) {
        return asPersistentMap().without(key);
    }

    class DBMapEntry implements IMapEntry, Sequential, Comparable {

        private int idx;

        DBMapEntry(int idx) {
            this.idx = idx;
        }

        /* Map.MapEntry */

        @Override
        public Object getKey() {
            return db.getKeywordByIdx(id, idx);
        }

        @Override
        public Object getValue() {
            return db.getValueByIdx(id, idx);
        }

        @Override
        public Object setValue(Object nv) {
            throw new UnsupportedOperationException("setValue");
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(Object o2) {
            if (!(o2 instanceof IMapEntry))
                throw new IllegalArgumentException
                        ("Can only compare against another LazyMapEntry.");
            return ((Comparable) getKey()).compareTo(((Entry) o2).getKey());
        }

        /* IMapEntry */

        @Override
        public Object key() {
            return db.getKeywordByIdx(id, idx);
        }

        @Override
        public Object val() {
            return db.getValueByIdx(id, idx);
        }

        @Override
        public String toString() {
            try {
                StringWriter sw = new StringWriter();
                sw.write('[');
                RT.print(getKey(), sw);
                sw.write(' ');
                RT.print(getValue(), sw);
                sw.write(']');
                return sw.toString();
            } catch (IOException e) {
                throw new RuntimeException("DBMapEntry toString exception.");
            }
        }

    }

    private class LazyIterator implements Iterator {
        private int idx = 0;
        private int size = size();

        @Override
        public boolean hasNext() {
            return (idx < size);
        }

        @Override
        public Object next() {
            return new DBMapEntry(idx++);
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException("DBMap is read-only, so remove on it's LazyInterator is not supported.");
        }
    }

    public IPersistentMap asPersistentMap() {
        IPersistentMap result = PersistentHashMap.create();
        Keyword[] keywords = getKeysAsKeywords();
        Object[] values = getValues();
        for (int i=0; i < keywords.length; i++) {
            result = result.assoc(keywords[i], values[i]);
        }
        return result;
    }
}
