package conceptual.core;

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

}
