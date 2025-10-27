package conceptual.util;

import java.util.Arrays;
import java.util.Comparator;
import conceptual.util.IntArrayList;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class VectorizedIntegerSets {

    public static int[] EMPTY = new int[0];

    public static Comparator<int[]> NULLS_FIRST_ARRAY_LENGTH_COMPARATOR =
        // Then, for non-null arrays, compare by length
        Comparator.nullsFirst((arr1, arr2) -> Integer.compare(arr1.length, arr2.length));

    private VectorizedIntegerSets() {}

    /**
     * Adds common elements to `intersection` using the traditional 2 pointer approach.
     * We use this when vector can't be filled which is the case when checking
     * the last segment of an array.
     */
    private static void populateIntersection(final int[] a, final int[] b,
                                             final int aStart, final int bStart,
                                             final IntArrayList intersection) {
        int i = aStart;
        int j = bStart;
        while (i < a.length && j < b.length) {
            int aVal = a[i];
            int bVal = b[j];
            if (aVal == bVal) {
                intersection.add(aVal);
                i++;
                j++;
            } else if (aVal < bVal) {
                // aVal is not in b, so look for next element from a
                i++;
            } else {
                // aVal is greater than bVal so bVal not in a, look for next element from b
                j++;
            }
        }
    }


    /*
    // NB keeping the impl around for reference and future testing
    // Contrary to my expectations, this is actually much slower than doing 2 vector ops
    private final static int[] intersectionLE(final int[] a, final int[] b) {
        if (null == a || null == b || 0 == a.length || 0 == b.length) { return EMPTY; }
        final IntArrayList intersection = new IntArrayList(a.length);
        final VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
        final int vecLength = species.length();
        final int lastVecIdx = vecLength - 1;
        final int maxJ = species.loopBound(b.length);

        int i = 0;
        int j = 0;

        while (i < a.length && j < maxJ && (j + vecLength) < b.length) {
            int valA = a[i];
            IntVector vA = IntVector.broadcast(species, valA);
            IntVector vB = IntVector.fromArray(species, b, j);
            VectorMask<Integer> leMask = vB.compare(VectorOperators.LE, vA);
            int lastTrueIdx = leMask.lastTrue();

            if (-1 == lastTrueIdx) {
                // Case 1: all items in vB are greater than valA, consider next item in A
                i++;
            } else if (valA == vB.lane(lastTrueIdx)) {
                // Case 2: equals last matching item in vB is equal to valA
                // also look at b 1 past where the match was found
                intersection.add(valA);
                i++;
                j = j + lastTrueIdx + 1;
            } else if (lastVecIdx == lastTrueIdx) {
                // Case 3: all less than valA so move full stride length
                j += vecLength;
            } else {
                // Case 4: Some items are less than valA
                i++;
                j = j + lastTrueIdx + 1;
            }
        }
        populateIntersection(a, b, i, j, intersection);
        return intersection.toIntArray();
    }
    */


    public final static int[] intersection(final int[] a, final int[] b) {
        if (null == a || null == b || 0 == a.length || 0 == b.length) { return EMPTY; }
        final IntArrayList intersection = new IntArrayList(a.length);
        final VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
        final int vecLength = species.length();
        final int maxJ = species.loopBound(b.length);

        int i = 0;
        int j = 0;

        while (i < a.length && j < maxJ && (j + vecLength) < b.length) {
            int valA = a[i];
            IntVector vA = IntVector.broadcast(species, valA);
            IntVector vB = IntVector.fromArray(species, b, j);
            VectorMask<Integer> eqMask = vB.eq(vA);
            VectorMask<Integer> ltMask = vB.lt(vA);

            // Case 1: equals
            if (eqMask.anyTrue()) {
                // found so add to intersection and get next item from a
                // also look at b 1 past where the match was found
                intersection.add(valA);
                i++;
                j = j + eqMask.firstTrue() + 1;
            } else if (ltMask.allTrue()) {
                // Case 2: all less than current so move full stride length
                j += vecLength;
            } else if (ltMask.anyTrue()) {
                // Case 3: some less than current (also some greater than current)
                i++;
                j = j + eqMask.lastTrue() + 1;
            } else {
                // all greater than current
                i++;
            }
        }
        populateIntersection(a, b, i, j, intersection);
        return intersection.toIntArray();
    }



    public final static int[] intersection(final int[][] sets) {
        if (null == sets || 0 == sets.length) { return EMPTY; }

        // sort sets by length so that smallest or null arrays are first
        Arrays.sort(sets, NULLS_FIRST_ARRAY_LENGTH_COMPARATOR);

        if (null == sets[0] || 0 == sets[0].length) { return EMPTY; }

        // maybe switch to IntArrayList if you can get it as a sorted set without sorting cause we know it's
        // already sorted
        int[] result = sets[0];
        for (int i = 1; i < sets.length; i++) {
            result = intersection(result, sets[i]);
        }
        return result;
    }
}
