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
