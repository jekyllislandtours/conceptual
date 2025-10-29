package conceptual.alpha;

import conceptual.util.IntArrayList;
import java.util.Arrays;
import java.util.Comparator;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * All methods in this class are NOT thread safe.
 */
public final class IntegerSetsAlpha {

    public static int[] EMPTY = new int[0];

    public static Comparator<int[]> NULLS_FIRST_ARRAY_LENGTH_COMPARATOR =
        // Then, for non-null arrays, compare by length
        Comparator.nullsFirst((a, b) -> Integer.compare(a.length, b.length));

    private IntegerSetsAlpha() {}


    /**
     * Answers if both arrays are null or have the same contents.
     * It is safe to pass nulls for either `a` or `b`.
     *
     * This method is NOT thread safe
     *
     * @param a
     * @param b
     * @return true if both arrays are null or have the same contents
     */
    public static boolean equals(int[] a, int[] b) {
        if (null==a && null==b) {
            return true;
        } else if (a != null && b != null && a.length == b.length) {
            for (int i=0; i < a.length; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }


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

    /**
     * Computes the set intersection of the sets `a` and `b`.
     * It is safe to pass `null` as the input as well as `null` sets in the array.
     *
     * This method is NOT thread safe.
     * This does NOT modify the input sets.
     *
     * @param sets array of sorted integer sets
     * @return the intersection of the input sets
     */
    public final static int[] intersection(final int[] a, final int[] b) {
        if (null == a || null == b || 0 == a.length || 0 == b.length) { return EMPTY; }

        final IntArrayList intersection = new IntArrayList(a.length);
        final VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
        final int vecLength = species.length();
        final int maxJ = species.loopBound(b.length);

        int i = 0;
        int j = 0;

        // feel like there's a way to get rid of the j + vecLength < b.length check here
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


    /**
     * Computes the set intersection of the input array of sorted integer sets.
     * It is safe to pass `null` as the input as well as `null` sets in the array.
     *
     * This method is NOT thread safe.
     * This does NOT modify the input sets.
     *
     * @param sets array of sorted integer sets
     * @return the intersection of the input sets
     */
    public final static int[] intersection(final int[][] sets) {
        if (null == sets || 0 == sets.length) { return EMPTY; }
        // shallow copy so we can sort the sets by length and not impact the callers data
        final int[][] xs = sets.clone();
        // sort sets by length so that smallest or null arrays are first
        Arrays.sort(xs, NULLS_FIRST_ARRAY_LENGTH_COMPARATOR);

        if (null == xs[0] || 0 == xs[0].length) { return EMPTY; }

        int[] result = xs[0];
        for (int i = 1; i < xs.length; i++) {
            result = intersection(result, xs[i]);
        }
        return result;
    }

    /**
     * Computes the set difference of the input array of sorted integer sets.
     * It is same to pass null as the input as well as null sets in the array.
     *
     * This method is NOT thread safe.
     * This does NOT modify the input sets.
     *
     * @param sets array of sorted integer sets
     * @return the intersection of the input sets
     */
    public final static int[] difference(final int[][] sets) {
        if (null == sets || 0 == sets.length) { return EMPTY; }
        if (null == sets[0] || 0 == sets[0].length) { return EMPTY; }

        int[] result = Arrays.copyOf(sets[0], sets[0].length);

        for (int i = 1; i < sets.length; i++) {
            final int[] elements = sets[i];
            if (elements == null || elements.length == 0) { continue; }

            int r = 0;
            int e = 0;

            // elements is not empty at this point

            while (r < result.length && e < elements.length) {
                final int valR = result[r];
                final int valE = elements[e];

                if (-1 == valR) {
                    // we already handled whatever was here move along
                    r++;
                } else if (valR < valE) {
                    // valR is less than valE which means valR can't be in elements and is part of the diff
                    // result[r] = valR;
                    r++;
                } else if (valR > valE) {
                    // valE is less than valR so its not yet in results
                    e++;
                } else {
                    // valR and valE are equal so move both forward
                    result[r] = -1;
                    r++;
                    e++;
                }
            }
        }

        // collect the results by ignoring the -1s
        int j = 0;
        for (int i = 0; i < result.length; i++) {
            int v = result[i];
            if (-1 != v) {
                result[j] = v;
                j++;
            }
        }

        return Arrays.copyOf(result, j);
    }

    /**
     * Returns the sum of all lengths.
     */
    private static int sumLengths(final int[][] sets) {
        int ans = 0;
        for (int i=0; i < sets.length; i++) {
            if (null != sets[i]) {
                ans += sets[i].length;
            }
        }
        return ans;
    }

    /**
     * Returns the set union of all the input sorted int sets.
     * It is safe to pass `null` as the input as well as `null` sets in the array.
     *
     * This method is NOT thread safe.
     * This does NOT modify the input sets.
     *
     * @param sets array of sorted integer sets
     * @return union of input sets
     */
    public final static int[] union(final int[][] sets) {
        if (null == sets || 0 == sets.length) { return EMPTY; }
        final int n = sumLengths(sets); // trying to prevent allocations along the way
        IntArrayList ial = new IntArrayList(n);
        for (int i=0; i < sets.length; i++) {
            ial.addAll(sets[i]);
        }
        return ial.toSortedIntSet();
    }
}
