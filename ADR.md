# Architecture Decision Record

## October 2025
Attempts to improve sorted integer set operations that were not successful are included here for reference.

### fastIntersection
This ended up being slower than the base implementation. Thought array allocation reduction would
speed things up, but that was definitely not the case.  Things slowed down significantly.

```java
    public final static int[] fastIntersection(final int[][] sets) {
        if (null == sets || 0 == sets.length) { return EMPTY; }

        // sort sets by length so that smallest or null arrays are first
        Arrays.sort(sets, NULLS_FIRST_ARRAY_LENGTH_COMPARATOR);

        if (null == sets[0] || 0 == sets[0].length) { return EMPTY; }

        int[] result = Arrays.copyOf(sets[0], sets[0].length);

        int writeIdx = 0;

        int rLength = result.length;

        for (int i = 1; i < sets.length; i++) {
            final int[] elements = sets[i];
            if (elements == null || elements.length == 0) { return EMPTY; }

            int r = 0;
            int e = 0;
            writeIdx = 0;

            while (r < rLength && e < elements.length) {
                final int valR = result[r];
                final int valE = elements[e];

                if (valR < valE) {
                    // Move r forward
                    r++;
                } else if (valR > valE) {
                    // Move e forward
                    e++;
                } else {
                    // Intersection found (valR == valE)
                    result[writeIdx] = valR;
                    writeIdx++;
                    r++;
                    e++;
                }
            }

            rLength = writeIdx;
        }

        if (0 == writeIdx) { return EMPTY; }
        return Arrays.copyOf(result, writeIdx);
    }
```

### intersectionLE
Using only the vectorized less than or equal operation proved to be slower than using
two separate vectorized ops; one for less than and another one for equal.  This implementation
was many times slower than the chosen implementation.

```java
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
```
### Faceting using Bitmaps
This approach proved much slower on real world data because you are doing far more additions.
For example, if there are 500 facets and 1 million concepts and each concept
has about 10 facets, now we're storing a bitmap representing 500 facets for
each concept and have to do 500 million addition ops instead of 50 million.
In addition, memory usage goes up significantly.
```java
    /**
     * Returns an int array with values 0 or 1. 1 is where the item from all was
     *
     * Given all:    `[325 326 327 328 329 330 331 332 333 334]`
     * Given chosen: `[327 333]`
     * Returns:      `[0 0 1 0 0 0 0 0 1 0]`
     * @param all sorted int set of all options
     * @param chosen sorted int set which is a subset of all or equal to all
     * @return an array of length all.length filled with 1s where chosen exists at all
     */
    public static int[] createBitmap(final int[] all, final int[] chosen) {
        if (null == chosen || 0 == chosen.length) { return null; }
        final int[] bitmap = new int[all.length];
        int i = 0;
        int j = 0;
        while (i < all.length && j < chosen.length) {
            final int av = all[i];
            final int cv = chosen[j];
            if (av == cv) {
                bitmap[i] = 1;
                i++;
                j++;
            } else if (av < cv) {
                i++;
            } else {
                j++; // this should never hit
            }
        }

        return bitmap;
    }

    /**
     * result and bitmap have to be the same length. Modifies
     * result by adding at each index the value from the same
     * index from bitmap. Returns the modified result.
     */
    public static int[] sumBitmaps(final int[] result, final int[] bitmap) {
        for (int i = 0; i < result.length; i++) {
            result[i] += bitmap[i];
        }
        return result;
    }

    public static int[] sumBitmaps(final DB db, final int bitmapId, final int bitmapBins, final int[] ids) {
        // assume it's a to-many relation
        // that the the bitmaps are all the same length and
        int[] result = new int[bitmapBins];
        for (int i = 0; i < ids.length; i++) {
            // an array containing either 0 or 1
            final int[] bitmap = (int[]) db.getValue(ids[i], bitmapId);
            if (null == bitmap || 0 == bitmap.length) { continue; }
            result = sumBitmaps(result, bitmap);
        }
        return result;
    }
```

### Vectorized Faceting using Bitmaps
This was also slow and rejected for the same reason as above.
```java

    /**
     * result and bitmap have to be the same length. Modifies
     * result by adding at each index the value from the same
     * index from bitmap. Returns the modified result.
     */
    public static int[] vSumBitmaps0(final int[] result, final int[] bitmap, final int maxI, final int vecLength) {
        int i = 0;
        while (i < maxI) {
            IntVector vA = IntVector.fromArray(IntVector.SPECIES_PREFERRED, result, i);
            IntVector vB = IntVector.fromArray(IntVector.SPECIES_PREFERRED, bitmap, i);
            IntVector vSum = vA.add(vB);
            vSum.intoArray(result, i);
            i += vecLength;
        }

        for(; i < result.length; i++) {
            result[i] += bitmap[i];
        }
        return result;
    }

    public static int[] vSumBitmaps(final DB db, final int bitmapId, final int bitmapBins, final int[] ids) {
        final VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
        final int vecLength = species.length();
        final int maxI = bitmapBins - vecLength;
        // assume it's a to-many relation
        // that the the bitmaps are all the same length and
        int[] result = new int[bitmapBins];
        for (int i = 0; i < ids.length; i++) {
            // an array containing either 0 or 1
            final int[] bitmap = (int[]) db.getValue(ids[i], bitmapId);
            if (null == bitmap || 0 == bitmap.length) { continue; }
            result = vSumBitmaps0(result, bitmap, maxI, vecLength);
        }
        return result;
    }
```
