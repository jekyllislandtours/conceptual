import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

/**
 * Computes the intersection of two sorted integer arrays using the Java Vector API.
 *
 * To compile and run this file, you MUST use the following flags:
 *
 * Compile:
 * javac --add-modules jdk.incubator.vector VectorSetIntersection.java
 *
 * Run:
 * java --add-modules jdk.incubator.vector VectorSetIntersection
 */
public class VectorSetIntersection {

    // Get the "best" vector shape for the current CPU.
    // This might be 128, 256, or 512 bits, holding 4, 8, or 16 ints.
    static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    static final int VEC_LEN = SPECIES.length();

    /**
     * Computes the intersection of two sorted integer arrays using a
     * SIMD-accelerated "galloping" search.
     *
     * @param a A sorted primitive integer array, assumed to be smaller than b.
     * @param b A sorted primitive integer array, assumed to be larger than a.
     * @return A new array containing only the elements present in both a and b.
     */
    public static int[] intersect(int[] a, int[] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return new int[0];
        }

        // User assumption: a is always smaller than b.

        // The result can be at most the size of the smaller array.
        int[] result = new int[a.length];
        int resultIndex = 0;

        int i = 0; // Pointer for a
        int j = 0; // Pointer for b

        // Get the "loop bound" for the large array.
        // This is the max index we can use for a full vector load.
        final int largeBound = SPECIES.loopBound(b.length);

        // --- Main Loop ---
        // Iterate through the smaller array.
        while (i < a.length && j < b.length) {
            int valA = a[i];

            // Broadcast the value from the small array into a full vector
            IntVector vA_broadcast = IntVector.broadcast(SPECIES, valA);

            // --- 1. Vector "Gallop" Phase ---
            // Use vectors to quickly skip chunks of the largeArray that are
            // entirely smaller than valA.
            while (j < largeBound) {
                // Load a vector from the large array
                IntVector vB = IntVector.fromArray(SPECIES, b, j);

                // Compare: vB < valA
                VectorMask<Integer> ltMask = vB.compare( VectorOperators.LT, vA_broadcast);

                if (ltMask.allTrue()) {
                    // All elements in vB are smaller than valA.
                    // We can safely skip the entire vector.
                    j += VEC_LEN;
                } else {
                    // At least one element in vB is >= valA.
                    // This is the chunk where valA might be.
                    // Check this chunk for an *exact* match.
                    VectorMask<Integer> eqMask = vB.compare(VectorOperators.EQ, vA_broadcast);

                    if (eqMask.anyTrue()) {
                        // --- MATCH FOUND (in vector) ---
                        result[resultIndex++] = valA;
                        // Advance j to *after* the match.
                        // Assumes unique elements (sets).
                        j += eqMask.firstTrue() + 1;
                    } else {
                        // No exact match in this vector.
                        // But some element was >= valA, so valA is not in the array.
                        // Advance j to the first element that was >= valA.
                        j += ltMask.firstTrue();
                    }
                    // Break from the vector gallop to move to the next 'i'
                    break;
                }
            }

            // --- 2. Scalar Search Phase ---
            // If we exited the vector loop because j >= largeBound,
            // we must check the remaining "tail" elements one by one.
            if (j >= largeBound) {
                while (j < b.length) {
                    int valB = b[j];
                    if (valB == valA) {
                        // --- MATCH FOUND (in tail) ---
                        result[resultIndex++] = valA;
                        j++;
                        break; // Exit scalar loop, move to next 'i'
                    } else if (valB > valA) {
                        // --- OVERSHOT (in tail) ---
                        // valA is not in the array
                        break; // Exit scalar loop, move to next 'i'
                    }
                    // valB < valA
                    j++;
                }
            }

            // Move to the next element in the small array
            i++;
        }

        // Trim the result array to its actual size.
        return Arrays.copyOf(result, resultIndex);
    }

    /**
     * Main method to demonstrate and test the intersection.
     */
    public static void main(String[] args) {
        // Test 1: Standard intersection
        int[] a = {1, 2, 5, 8, 9, 10, 12, 13, 15, 20, 22, 24, 25, 28, 30};
        int[] b = {2, 4, 6, 8, 10, 14, 15, 20, 21, 22, 23, 26, 27, 28, 29, 30};

        System.out.println("Vector API (" + SPECIES.length() * 32 + "-bit) intersection:");
        System.out.println("A: " + Arrays.toString(a));
        System.out.println("B: " + Arrays.toString(b));

        int[] intersection = intersect(a, b);
        System.out.println("Result:   " + Arrays.toString(intersection));
        // Expected: [2, 8, 10, 15, 20, 22, 28, 30]

        System.out.println("---");

        // Test 2: No matches
        int[] c = {1, 3, 5};
        int[] d = {2, 4, 6};
        System.out.println("C: " + Arrays.toString(c));
        System.out.println("D: " + Arrays.toString(d));
        int[] intersection2 = intersect(c, d);
        System.out.println("Result:   " + Arrays.toString(intersection2));
        // Expected: []

        System.out.println("---");

        // Test 3: One array is a subset
        int[] e = {10, 20, 30};
        int[] f = {5, 10, 15, 20, 25, 30, 35};
        System.out.println("E: " + Arrays.toString(e));
        System.out.println("F: " + Arrays.toString(f));
        int[] intersection3 = intersect(e, f);
        System.out.println("Result:   " + Arrays.toString(intersection3));
        // Expected: [10, 20, 30]
    }
}




