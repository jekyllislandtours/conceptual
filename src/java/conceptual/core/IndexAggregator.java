package conceptual.core;

import conceptual.util.IntegerSets;

import java.util.*;

public class IndexAggregator {

    Map<Integer, List<Integer>> indexMap = new HashMap<>();

    // TODO constructor sort and remove dups

    public void add(int key, int id) {
        List<Integer> index = indexMap.get(key);
        //if (index == null) index = new LinkedList<>();
        if (index == null) index = new ArrayList<>();
        index.add(id);
        indexMap.put(key, index);
    }

    public int[] keys() {
        int[] result = null;
        Set<Integer> keys = indexMap.keySet();
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
        return IntegerSets.listToIntArray(indexMap.get(key));
    }
}
