package conceptual.core;

import clojure.lang.*;
import conceptual.util.IntegerSets;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * This is a materialized/projected implementation of a map facade on the idea of a concept.
 */
public class ProjectedMap extends DBMap {

    protected final int[] mat_ks;

    ProjectedMap(DB db, int id) {
        super(db, id);
        mat_ks = db.getKeys(id);
    }

    ProjectedMap(DB db, int id, int[] projection) {
        super(db, id);
        mat_ks = IntegerSets.intersection(db.getKeys(id), projection);
    }

    /* DBMap specific properties */

    public int getId() {
        return id;
    }

    public int[] getKeys() {
        return mat_ks;
    }

    public Object[] getValues() {
        Object[] result = new Object[mat_ks.length];
        for (int i=0; i < result.length; i++) {
            result[i] = db.getValue(id, mat_ks[i]);
        }
        return result;
    }

    /* Map */

    /** Returns true if this map contains a mapping for the specified key. */
    @Override
    public boolean containsKey(Object key) {
        return IntegerSets.binarySearch(mat_ks, db.keyToId(key), 0, mat_ks.length) > 1;
    }

    /** Returns the value to which the specified key is mapped, or null if
     * this map contains no mapping for the key. */
    @Override
     public Object get(Object key) {
        int kid = db.keyToId(key);
        if (kid > -1) {
            return get(kid);
        } else return null;
    }

    /**
     * Returns the value for a given key.
     *
     * @param key the id of the key for the value to be returned.
     * @return Object the value for the given key.
     */
    public Object get(int key) {
        final int idx = IntegerSets.contains(mat_ks, key);
        return idx > -1 ? db.getValue(id, key) : null;
    }

    /** Returns the number of key-value mappings in this map. */
    @Override
    public int size() {
        return mat_ks.length;
    }

    /** Returns a Set view of the mappings contained in this map. */
    @Override
    public Set entrySet() {
        return new AbstractSet() {
            public Iterator iterator() {
                return ProjectedMap.this.iterator();
            }

            public int size() {
                return ProjectedMap.this.size();
            }

            public int hashCode() {
                return ProjectedMap.this.hashCode();
            }

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

    /* Iterable */

    @Override
    public Iterator iterator() {
        return new ProjectedIterator();
    }

        /* ISeq */

    @Override
    public ISeq seq() {
        return IteratorSeq.create(new ProjectedIterator());
    }

    /* Associative */

    @Override
    public IMapEntry entryAt(Object key) {
        final int kid = db.keyToId(key);
        final int idx = IntegerSets.binarySearch(mat_ks, kid, 0, mat_ks.length);
        return (kid > -1 && idx > -1) ? new ProjectedMapEntry(idx) : null;
    }

    class ProjectedMapEntry implements IMapEntry, Comparable {

        private int idx;

        ProjectedMapEntry(int idx) {
            this.idx = idx;
        }

        /* Map.MapEntry */

        @Override
        public Object getKey() {
            return db.getKeyword(mat_ks[idx]);
        }

        @Override
        public Object getValue() { return db.getValue(id, mat_ks[idx]); }

        @Override
        public Object setValue(Object nv) {
            throw new UnsupportedOperationException("setValue");
        }

        @Override
        public int compareTo(Object o2) {
            if (!(o2 instanceof IMapEntry))
                throw new IllegalArgumentException
                        ("Can only compare against another LazyMapEntry.");
            return ((Comparable) getKey()).compareTo(((Entry) o2).getKey());
        }

        /* IMapEntry */

        @Override
        public Object key() {
            return getKey();
        }

        @Override
        public Object val() {
            return getValue();
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

    private class ProjectedIterator implements Iterator {
        private int idx = 0;
        private int size = size();

        @Override
        public boolean hasNext() {
            return (idx < size);
        }

        @Override
        public Object next() {
            return new ProjectedMapEntry(idx++);
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
