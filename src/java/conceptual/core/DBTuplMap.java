package conceptual.core;

import clojure.lang.*;

import org.cojen.tupl.io.Utils;

import conceptual.util.IntegerSets;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * This is a lazy implementation of a map facade on the idea of a concept.
 */
public class DBTuplMap extends DBMap implements ILookup, Map, Iterable, Associative, Counted {

    protected byte[] idBytes = new byte[4];

    public DBTuplMap(DB db, int id) {
        super(db, id);
        Utils.encodeIntBE(idBytes, 0, id);
    }

    /* DBMap specific properties */

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Keyword getKeyword() {
        return (Keyword) get(DB.KEY_ID);
    }

    @Override
    public Object getType() {
        return get(DB.TYPE_ID);
    }

    public Keyword[] getKeysAsKeywords() {
        return db.getKeysAsKeywords(id);
    }

    public int[] getKeys() {
        return ((TuplDB) db).getKeys(idBytes);
    }

    public Object[] getValues() {
        return db.getValues(id);
    }

    /* Map */

    /** Removes all of the mappings from this map (optional operation). */
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
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

        DBTuplMap dbMap = (DBTuplMap) o;

        return id != dbMap.id && db.equals(dbMap.db);
    }

    /** Returns the hash code value for this map. */
    @Override
    public int hashCode() {
        return id;
    }

    public Object getValue(final int key) {
        return db.getValue(id, key);
    }

    /** Returns the value to which the specified key is mapped, or null if
     * this map contains no mapping for the key. */
    @Override
     public Object get(Object key) {
        int kid = db.keyToId(key);
        if (kid > -1) {
            return getValue(kid);
        } else return null;
    }

    /**
     * Returns the value for a given key.
     *
     * @param key the id of the key for the value to be returned.
     * @return Object the value for the given key.
     */
    public Object get(int key) {
        return getValue(key);
    }

    /** Returns true if this map contains no key-value mappings. */
    @Override
    public boolean isEmpty() {
        return (size() == 0);
    }

    /** Returns a Set view of the keys contained in this map. */
    @Override
    public Set keySet() {
        return new LinkedHashSet<>(Arrays.asList(getKeysAsKeywords()));
    }

    /** Associates the specified value with the specified key in this map (optional operation). */
    @Override
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    /** Copies all of the mappings from the specified map to this map (optional operation). */
    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    /** Removes the mapping for a key from this map if it is present (optional operation). */
    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    /** Returns the number of key-value mappings in this map. */
    @Override
    public int size() {
        return getKeys().length;
    }

    /** Returns a Set view of the mappings contained in this map. */
    @Override
    public Set entrySet() {
        return new AbstractSet() {
            @Override
            public Iterator iterator() {
                return DBTuplMap.this.iterator();
            }

            @Override
            public int size() {
                return DBTuplMap.this.size();
            }

            @Override
            public int hashCode() {
                return DBTuplMap.this.hashCode();
            }

            @Override
            public boolean contains(Object o){
                throw new UnsupportedOperationException();
            }
        };
    }

    /** Returns a Collection view of the values contained in this map. */
    @Override
    public Collection values() {
        return Arrays.asList(getValues());
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
    public Object invoke(Object arg1, Object notFound) {
        return valAt(arg1, notFound);
    }

    /* ISeq */

    @Override
    public ISeq seq() {
        return IteratorSeq.create(new LazyIterator());
    }

    @Override
    public IPersistentCollection cons(Object o) {
        throw new UnsupportedOperationException();
        //return asPersistentMap().cons(o);
    }

    @Override
    public IPersistentCollection empty() {
        throw new UnsupportedOperationException();
        //return PersistentHashMap.create();
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
        if (kid > -1) {
            return new DBMapEntry(kid);
        } return null;
    }

    @Override
    public Associative assoc(Object key, Object val) {
        throw new UnsupportedOperationException();
        //return asPersistentMap().assoc(key, val);
    }

    class DBMapEntry implements IMapEntry, Comparable {
        private int key;

        DBMapEntry(int key) {
            this.key = key;
        }

        /* Map.MapEntry */

        @Override
        public Object getKey() {
            return key();
        }

        @Override
        public Object getValue() {
            return val();
        }

        @Override
        public Object setValue(Object nv) {
            throw new UnsupportedOperationException("setValue");
        }

        @Override
        public int compareTo(Object o2) {
            if (!(o2 instanceof DBMapEntry))
                throw new IllegalArgumentException
                        ("Can only compare against another LazyMapEntry.");
            return ((Comparable) getKey()).compareTo(((DBMapEntry) o2).getKey());
        }

        /* IMapEntry */

        @Override
        public Object key() {
            return db.getValue(key, DB.KEY_ID);
        }

        @Override
        public Object val() { return db.getValue(id, key); }

        @Override
        public String toString() {
            try {
                StringWriter sw = new StringWriter();
                sw.write('[');
                RT.print(key(), sw);
                sw.write(' ');
                RT.print(val(), sw);
                sw.write(']');
                return sw.toString();
            } catch (Exception e) {
                throw new RuntimeException("DBMapEntry toString exception.");
            }
        }
    }

    class LazyIterator implements Iterator {
        private int idx = 0;
        private int size = size();
        private int[] keys = null;

        @Override
        public boolean hasNext() {
            return (idx < size);
        }

        @Override
        public Object next() {
            if (keys == null) keys = getKeys();
            return new DBMapEntry(keys[idx++]);
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException();
        }
    }

    public IPersistentMap asPersistentMap() {
        IPersistentMap result = PersistentHashMap.create();
        Keyword[] keywords = db.getKeysAsKeywords(id);
        Object[] values = getValues();
        for (int i=0; i < keywords.length; i++) {
            result = result.assoc(keywords[i], values[i]);
        }
        return result;
    }
}
