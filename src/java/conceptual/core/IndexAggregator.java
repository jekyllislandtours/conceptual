package conceptual.core;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;
import conceptual.util.IntegerSets;

import java.util.*;

public class IndexAggregator {

    Map<Integer, List<Integer>> indexAddMap = new HashMap<>();
    Map<Integer, List<Integer>> indexRemoveMap = new HashMap<>();

    // TODO constructor sort and remove dups

    public void add(int key, int id) {
        List<Integer> index = indexAddMap.get(key);
        //if (index == null) index = new LinkedList<>();
        if (index == null) index = new ArrayList<>();
        index.add(id);
        indexAddMap.put(key, index);
    }

    public void remove(int key, int id) {
        List<Integer> index = indexRemoveMap.get(key);
        //if (index == null) index = new LinkedList<>();
        if (index == null) index = new ArrayList<>();
        index.add(id);
        indexRemoveMap.put(key, index);
    }

    public int[] keys() {
        int[] result = null;
        Set<Integer> keys = indexAddMap.keySet();
        int size = keys.size();
        if (size > 0) {
            result = new int[size];
            int i=0;
            Iterator<Integer> iterator = keys.iterator();
            while (iterator.hasNext()) {
                result[i++] = iterator.next();
            }
        }
        return result;
    }

    public int[] removeKeys() {
        int[] result = null;
        Set<Integer> keys = indexRemoveMap.keySet();
        int size = keys.size();
        if (size > 0) {
            result = new int[size];
            int i=0;
            Iterator<Integer> iterator = keys.iterator();
            while (iterator.hasNext()) {
                result[i++] = iterator.next();
            }
        }
        return result;
    }

    // TODO sort and remove dups
    public int[] ids(int key) {
        return IntegerSets.listToIntArray(indexAddMap.get(key));
    }

    public int[] removeIds(int key) {
        int[] result = null;
        List<Integer> removeList = indexRemoveMap.get(key);
        if (removeList != null) {
            result = IntegerSets.listToIntArray(removeList);
        } else {
            result = new int[] {};
        }
        return result;
    }

    public static IPersistentMap updateIndex(final IPersistentMap index, final int id, final int key, final Object val) {
        return index.assoc(key, ((IPersistentMap) index.valAt(key, PersistentHashMap.EMPTY)).without(val).assoc(val, id));
    }

    public static IPersistentMap removeFromIndex(final IPersistentMap index, final int key, final Object val) {
        return index.assoc(key, ((IPersistentMap) index.valAt(key, PersistentHashMap.EMPTY)).without(val));
    }

    public static IPersistentMap updateIndices(final IPersistentMap indices,
                                               final int id,
                                               final int[] keys,
                                               final boolean[] key_tags,
                                               final Object[] vals) {
        IPersistentMap result = indices;
        if (keys.length > 0) {
            for (int i=0; i < keys.length; i++) {
                if (key_tags[i]) {
                    result = updateIndex(result, id, keys[i], vals[i]);
                }
            }
        }
        return result;
    }

    public static IPersistentMap removeFromIndices(final IPersistentMap indices,
                                                   final int[] keys,
                                                   final boolean[] key_tags,
                                                   final Object[] vals) {
        IPersistentMap result = indices;
        if (keys.length > 0) {
            for (int i=0; i < keys.length; i++) {
                if (key_tags[i]) {
                    result = removeFromIndex(result, keys[i], vals[i]);
                }
            }
        }
        return result;
    }
}
