package conceptual.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

/**
 * IntegerSets assumes all incoming sets have no repeats and are sorted from least to
 */
public final class IntegerSets {

    private IntegerSets() {}

    /**
     * <p>getIntersectionAndUnionCount</p>
     *
     * @param setA an array of int.
     * @param setB an array of int.
     * @return an array of int.
     */
    public final static int[] getIntersectionAndUnionCount(final int[] setA, final int[]setB) {
        int[] pair = new int[] {0, 0};
        int i=0, j=0;
        //System.out.println("lengths:" + setA.length + " " + setB.length);
        if (setA != null && setA.length > 0 && setB != null && setB.length > 0) {
            while (true) {
                if (i == setA.length) {
                    if (j == setB.length) break;
                    pair[1]++;
                    if (j < setB.length) j++;
                } else if (j == setB.length || setA[i] < setB[j]) {
                    pair[1]++;
                    if (i < setA.length) i++;
                } else if (setA[i] > setB[j]) {
                    pair[1]++;
                    if (j < setB.length) j++;
                } else if (setA[i] == setB[j]) {
                    pair[0]++;
                    pair[1]++;
                    if (i < setA.length) i++;
                    if (j < setB.length) j++;
                }
                //System.out.println(i + " " + j + " " + pair[0] + " " + pair[1]);
            }
        } else if (setA != null && setA.length > 0) {
            pair[1] = setA.length;
        } else if (setB != null && setB.length > 0) {
            pair[1] = setB.length;
        }
        //System.out.println();
        return pair;
    }

    public final static int[] intersection(final int[][] sets) {
        return getIntersection(sets);
    }

    /**
     * <p>getIntersection</p>
     *
     * @param sets an array of int.
     * @return an array of int.
     */
    public final static int[] getIntersection(final int[]... sets) {
        int smallestLength = sets[0].length;
        int smallestIndex = 0;
        for (int i=1; i < sets.length; i++) {
            if (sets[i].length < smallestLength) {
                smallestIndex = i;
                smallestLength = sets[i].length;
            }
        }
        int[] result = sets[smallestIndex];
        for (int i=0; i < sets.length; i++) {
            if (i != smallestIndex) {
                result = getIntersection(result, sets[i]);
            }
            if (result.length == 0) break;
        }
        return result;
    }

    public final static int[] union(final int[][] sets) {
        return getUnion(sets);
    }

    /**
     * <p>getUnion</p>
     *
     * @param sets an array of int.
     * @return an array of int.
     */
    public final static int[] getUnion(final int[]... sets) {
        int[] result = new int[] {};
        for (int i=0; i < sets.length; i++) {
            result = getUnion(result, sets[i]);
        }
        return result;
    }

    public final static int[] intersection(int[] setA, int[] setB) {
        return getIntersection(setA, setB);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC) {
        return getIntersection(setA, setB, setC);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD) {
        return getIntersection(setA, setB, setC, setD);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE) {
        return getIntersection(setA, setB, setC, setD, setE);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF) {
        return getIntersection(setA, setB, setC, setD, setE, setF);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG) {
        return getIntersection(setA, setB, setC, setD, setE, setF, setG);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG, int[] setH) {
        return getIntersection(setA, setB, setC, setD, setE, setF, setG, setH);
    }

    public final static int[] intersection(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG, int[] setH, int[] setI) {
        return getIntersection(setA, setB, setC, setD, setE, setF, setG, setH, setI);
    }

    /**
     * <p>getIntersection</p>
     *
     * @param setA an array of int.
     * @param setB an array of int.
     * @return an array of int.
     */
    public final static int[] getIntersection(int[] setA, int[] setB) {
        int[] intersection = null;
        int i=0, j=0;
        //System.out.println("lengths:" + setA.length + " " + setB.length);
        if (setA != null && setA.length > 0 && setB != null && setB.length > 0) {
            int count = 0;
            if (setA.length <= setB.length) {
                intersection = new int[setA.length];
            } else {
                intersection = new int[setB.length];
            }
            while (true) {
                if (i == setA.length) {
                    if (j == setB.length) break; // exit condition
                    if (j < setB.length) j++;
                } else if (j == setB.length || setA[i] < setB[j]) {
                    if (i < setA.length) i++;
                } else if (setA[i] > setB[j]) {
                    if (j < setB.length) j++;
                } else if (setA[i] == setB[j]) {
                    intersection[count] = setA[i];
                    count++;
                    if (i < setA.length) i++;
                    if (j < setB.length) j++;
                }
            }
            int[] temp = new int[count];
            System.arraycopy(intersection, 0, temp, 0, count);
            intersection = temp;
        } else if (setA != null && setA.length > 0) {
            intersection = new int[] {};
        } else if (setB != null && setB.length > 0) {
            intersection = new int[] {};
        }
        return intersection;
    }

    public final static int[] union(final int[] setA, final int[] setB) {
        return getUnion(setA, setB);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC) {
        return getUnion(setA, setB, setC);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD) {
        return getUnion(setA, setB, setC, setD);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE) {
        return getUnion(setA, setB, setC, setD, setE);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF) {
        return getUnion(setA, setB, setC, setD, setE, setF);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG) {
        return getUnion(setA, setB, setC, setD, setE, setF, setG);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG, int[] setH) {
        return getUnion(setA, setB, setC, setD, setE, setF, setG, setH);
    }

    public final static int[] union(int[] setA, int[] setB, int[] setC, int[] setD, int[] setE, int[] setF, int[] setG, int[] setH, int[] setI) {
        return getUnion(setA, setB, setC, setD, setE, setF, setG, setH, setI);
    }

    /**
     * <p>getUnion</p>
     *
     * @param setA an array of int.
     * @param setB an array of int.
     * @return an array of int.
     */
    public final static int[] getUnion(final int[] setA, final int[] setB) {
        int[] sets = null;
        int i=0, j=0;
        //System.out.println("lengths:" + setA.length + " " + setB.length);
        if (setA != null && setA.length > 0 && setB != null && setB.length > 0) {
            int count = 0;
            sets = new int[setA.length + setB.length];
            while (true) {
                if (i == setA.length) {
                    if (j == setB.length) break; // exit condition
                    sets[count] = setB[j];
                    count++;
                    if (j < setB.length) j++;
                } else if (j == setB.length || setA[i] < setB[j]) {
                    sets[count] = setA[i];
                    count++;
                    if (i < setA.length) i++;
                } else if (setA[i] > setB[j]) {
                    sets[count] = setB[j];
                    count++;
                    if (j < setB.length) j++;
                } else if (setA[i] == setB[j]) {
                    sets[count] = setA[i];
                    count++;
                    if (i < setA.length) i++;
                    if (j < setB.length) j++;
                }
            }
            int[] temp = new int[count];
            System.arraycopy(sets, 0, temp, 0, count);
            sets = temp;
        } else if (setA != null && setA.length > 0) {
            sets = new int[setA.length];
            System.arraycopy(setA, 0, sets, 0, setA.length);
        } else if (setB != null && setB.length > 0) {
            sets = new int[setB.length];
            System.arraycopy(setB, 0, sets, 0, setB.length);
        }
        return sets;
    }

    public final static int[] getUnion(final int[] set, int item) {
        final int[] result;
        if (set != null) {
            final int idx = IntegerSets.binarySearchGreater(set, item);
            result = new int[set.length + 1];
            if (idx == set.length) { // update end
                System.arraycopy(set, 0, result, 0, set.length);
                result[set.length] = item;
            } else { // update mid
                System.arraycopy(set, 0, result, 0, idx);
                result[idx] = item;
                System.arraycopy(set, idx, result, (idx + 1), (set.length - idx));
            }
        } else {
            result = new int[] {item};
        }
        return result;
    }

    public final static int[] union(final int[] set, int item) {
        return getUnion(set, item);
    }


    public final static int[] minus(final int[] setA, final int[] setB) {
        return getDifference(setA, setB);
    }

    public final static int[] diff(final int[] setA, final int[] setB) {
        return getDifference(setA, setB);
    }

    public final static int[] difference(final int[] setA, final int[] setB) {
        return getDifference(setA, setB);
    }

    /**
     * <p>getDifference</p>
     *
     * @param setA an array of int.
     * @param setB an array of int.
     * @return an array of int.
     */
    public final static int[] getDifference(final int[] setA, final int[] setB) {
        int[] difference = null;
        int i=0, j=0;
        //System.out.println("lengths:" + setA.length + " " + setB.length);
        if (setA != null && setA.length > 0 && setB != null && setB.length > 0) {
            int count = 0;
            difference = new int[setA.length];
            while (true) {
                if (i == setA.length) {
                    break; // exit condition
                } else if (j == setB.length || setA[i] < setB[j]) {
                    difference[count] = setA[i];
                    count++;
                    if (i < setA.length) i++;
                } else if (setA[i] > setB[j]) {
                    // b is not in a
                    if (j < setB.length) j++;
                } else if (setA[i] == setB[j]) {
                    // do not count... this is difference
                    if (i < setA.length) i++;
                    if (j < setB.length) j++;
                }
            }
            int[] temp = new int[count];
            System.arraycopy(difference, 0, temp, 0, count);
            difference = temp;
        } else if (setA != null && setA.length > 0) {
            difference = new int[setA.length];
            System.arraycopy(setA, 0, difference, 0, setA.length);
        } else if (setB != null && setB.length > 0) {
            difference = new int[] {};
        }
        return difference;
    }

    /**
     * Returns the index of a key in a sorted int set, -1 if not found
     *
     * @param sortedIntSet an array of int.
     * @param key a int.
     * @return a int.
     */
    public static int contains(int[] sortedIntSet, int key) {
        return binarySearch
            (sortedIntSet, key, 0, sortedIntSet.length);
    }

    /**
     * The result is a two dimensional array.
     * result[0] is the intersection array
     * result[1] is the union array
     * @param setA
     * @param setB
     * @return
     */
    /*
    public final static int[][] getIntersectionAndUnion(final int[] setA, final int[] setB) {
        int[][] sets = new int[2][];
        int i=0, j=0;
        //System.out.println("lengths:" + setA.length + " " + setB.length);
        if (setA != null && setA.length > 0 && setB != null && setB.length > 0) {
            int[] count = new int[] {0, 0};
            if (setA.length <= setB.length) {
                sets[0] = new int[setA.length];
            } else {
                sets[0] = new int[setB.length];
            }
            sets[1] = new int[setA.length + setB.length];
            while (true) {
                if (i == setA.length) {
                    if (j == setB.length) break; // exit condition
                    sets[1][count[1]] = setB[j];
                    count[1]++;
                    if (j < setB.length) j++;
                } else if (j == setB.length || setA[i] < setB[j]) {
                    sets[1][count[1]] = setA[i];
                    count[1]++;
                    if (i < setA.length) i++;
                } else if (setA[i] > setB[j]) {
                    sets[1][count[1]] = setB[j];
                    count[1]++;
                    if (j < setB.length) j++;
                } else if (setA[i] == setB[j]) {
                    sets[1][count[0]] = setA[i];
                    sets[1][count[1]] = setA[i];
                    count[0]++;
                    count[1]++;
                    if (i < setA.length) i++;
                    if (j < setB.length) j++;
                }
            }
            int[] intersection = new int[count[0]];
            System.arraycopy(sets[0], 0, intersection, 0, count[0]);
            sets[0] = intersection;
            int[] union = new int[count[0]];
            System.arraycopy(sets[1], 0, union, 0, count[1]);
            sets[1] = union;
        } else if (setA != null && setA.length > 0) {
            sets[0] = new int[] {};
            sets[1] = new int[setA.length];
            System.arraycopy(setA, 0, sets[1], 0, setA.length);
        } else if (setB != null && setB.length > 0) {
            sets[0] = new int[] {};
            sets[1] = new int[setB.length];
            System.arraycopy(setB, 0, sets[1], 0, setB.length);
        }
        return sets;
    }
    */

    /**
     * Searches for a key in a sorted array, and returns an index to an element
     * which is greater than or equal key.
     *
     * @param index
     *            Sorted array of integers
     * @param key
     *            Search for something equal or greater
     * @param begin
     *            Start posisiton in the index
     * @param end
     *            One past the end position in the index
     * @return end if nothing greater or equal was found, else an index
     *         satisfying the search criteria
     */
    public static int binarySearchGreater(int[] index, int key, int begin,
            int end) {
        return binarySearchInterval(index, key, begin, end, true);
    }

    /**
     * Searches for a key in a sorted array, and returns an index to an element
     * which is greater than or equal key.
     *
     * @param index
     *            Sorted array of integers
     * @param key
     *            Search for something equal or greater
     * @return index.length if nothing greater or equal was found, else an index
     *         satisfying the search criteria
     */
    public static int binarySearchGreater(int[] index, int key) {
        return binarySearchInterval(index, key, 0, index.length, true);
    }

    /**
     * Searches for a key in a sorted array, and returns an index to an element
     * which is smaller than or equal key.
     *
     * @param index
     *            Sorted array of integers
     * @param key
     *            Search for something equal or greater
     * @param begin
     *            Start posisiton in the index
     * @param end
     *            One past the end position in the index
     * @return begin-1 if nothing smaller or equal was found, else an index
     *         satisfying the search criteria
     */
    public static int binarySearchSmaller(int[] index, int key, int begin,
            int end) {
        return binarySearchInterval(index, key, begin, end, false);
    }

    /**
     * Searches for a key in a sorted array, and returns an index to an element
     * which is smaller than or equal key.
     *
     * @param index
     *            Sorted array of integers
     * @param key
     *            Search for something equal or greater
     * @return -1 if nothing smaller or equal was found, else an index
     *         satisfying the search criteria
     */
    public static int binarySearchSmaller(int[] index, int key) {
        return binarySearchInterval(index, key, 0, index.length, false);
    }

    /**
     * Searches for a key in a subset of a sorted array.
     *
     * @param index
     *            Sorted array of integers
     * @param key
     *            Key to search for
     * @param begin
     *            Start posisiton in the index
     * @param end
     *            One past the end position in the index
     * @return Integer index to key. -1 if not found
     */
    public static int binarySearch(int[] index, int key, int begin, int end) {
        end--;

        while (begin <= end) {
            int mid = (end + begin) >> 1;

            if (index[mid] < key)
                begin = mid + 1;
            else if (index[mid] > key)
                end = mid - 1;
            else
                return mid;
        }

        return -1;
    }

    private static int binarySearchInterval(int[] index,
                                            int key,
                                            int begin,
                                            int end,
                                            boolean greater) {

        // Zero length array?
        if (begin == end)
            if (greater)
                return end;
            else
                return begin - 1;

        end--; // Last index
        int mid = (end + begin) >> 1;

        // The usual binary search
        while (begin <= end) {
            mid = (end + begin) >> 1;

            if (index[mid] < key)
                begin = mid + 1;
            else if (index[mid] > key)
                end = mid - 1;
            else
                return mid;
        }

        // No direct match, but an inf/sup was found
        if ((greater && index[mid] >= key) || (!greater && index[mid] <= key))
            return mid;
        // No inf/sup, return at the end of the array
        else if (greater)
            return mid + 1; // One past end
        else
            return mid - 1; // One before start
    }

    /**
     * Finds the number of repeated entries
     *
     * @param num
     *            Maximum index value
     * @param ind
     *            Indices to check for repetitions
     * @return Array of length <code>num</code> with the number of repeated
     *         indices of <code>ind</code>
     */
    public static int[] bandwidth(int num, int[] ind) {
        int[] nz = new int[num];

        for (int i = 0; i < ind.length; ++i)
            nz[ind[i]]++;

        return nz;
    }

    /**
     * <p>listToIntArray</p>
     *
     * @param list a {@link java.util.List} object.
     * @return an array of int.
     */
    public static int[] listToIntArray(List<Integer> list) {
        int[] result = null;
        int size = list.size();
        if (size > 0) {
            // make primitive array
            int[] temp = new int[size];
            ListIterator<Integer> iterator = list.listIterator();
            for (int i=0; iterator.hasNext(); i++) {
                temp[i] = iterator.next();
            }
            // sort and filter dups
            result = sortAndFilterDuplicates(temp);
        }
        return result;
    }

    public static int[] treeSetToSortedIntArray(TreeSet<Integer> set) {
        int[] result = null;
        int size = set.size();
        if (size > 0) {
            // make primitive array
            result = new int[size];
            Iterator<Integer> iterator = set.iterator();
            for (int i=0; iterator.hasNext(); i++) {
                result[i] = iterator.next();
            }
        }
        return result;
    }

    // sorts and filters duplicates in array
    /**
     * <p>sortAndFilterDuplicates</p>
     *
     * @param array an array of int.
     * @return an array of int.
     */
    public static int[] sortAndFilterDuplicates(int[] array) {
        int[] result = null;
        // do not sort original array
        int[] clone = clone(array);
        // first sort
        Arrays.sort(clone);
        // filter out dups
        int index = 0;
        int[] temp = new int[clone.length];
        for (int i=0; i < clone.length; i++) {
            if (index == 0 || (index > 0 && clone[i] > temp[index - 1])) {
                temp[index] = clone[i];
                index++;
            }// else if (index > 0 && array[i] == temp[index - 1]) {
                // dups are expected... filtering them out now.
                //log("yes there can be dups.");
            //} else {
            //    System.out.println("baddata.com!!!... help");
            //}
        }
        // copy into result
        if (index > 0) {
            result = new int[index];
            System.arraycopy(temp, 0, result, 0, index);
        }
        return result;
    }

    /**
     * <p>equals</p>
     *
     * @param a an array of int.
     * @param b an array of int.
     * @return a boolean.
     */
    public static boolean equals(int[] a, int[] b) {
        if (a != null && b != null && a.length == b.length) {
            for (int i=0; i < a.length; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        } else if (a == null && b == null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * <p>clone</p>
     *
     * @param a an array of int.
     * @return an array of int.
     */
    public static int[] clone(int[] a) {
        int[] result = null;
        if (a != null) {
            if (a.length > 0) {
                result = new int[a.length];
                System.arraycopy(a, 0, result, 0, a.length);
            } else {
                result = new int[] {};
            }
        }
        return result;
    }

    /**
     * <p>printArray</p>
     *
     * @param label a {@link java.lang.String} object.
     * @param set an array of int.
     */
    public static void printArray(String label, int[] set) {
        StringBuilder builder = new StringBuilder();
        builder.append(label).append(" {");
        for (int i=0; i < set.length; i++) {
            builder.append(set[i]);
            if (i < set.length - 1) {
                builder.append(", ");
            }
        }
        builder.append("}");
        System.out.println(builder.toString());
    }

    /**
     * <p>main</p>
     *
     * @param args an array of {@link java.lang.String} objects.
     */
    public static void main(String[] args) {
        int size = 10000;
        int maxIter = 100000;
        double p = 0.5;

        int[] a = new int[size];
        int[] b = new int[size];

        Random random = new Random();
        int aCount = 0;
        int bCount = 0;
        for (int i=0; i < size; i++) {
            if (random.nextDouble() > (1.0d - p)) {
                a[aCount] = i;
                aCount++;
            }
            if (random.nextDouble() > (1.0d - p)) {
                b[bCount] = i;
                bCount++;
            }
        }
        int[] tempA = new int[aCount];
        System.arraycopy(a, 0, tempA, 0, aCount);
        a = tempA;
        int[] tempB = new int[bCount];
        System.arraycopy(b, 0, tempB, 0, bCount);
        b = tempB;

        printArray("a (" + a.length + ")", a);
        printArray("b (" + b.length + ")", b);

        long startTime = 0;
        long endTime = 0;
        long difference = 0;

        int[] set = null;

        set = getIntersection(a, b);
        printArray("getIntersection (" + set.length + ")", set);
        startTime = System.currentTimeMillis();
        for (int i=0; i < maxIter; i++) {
            set = getIntersection(a, b);
        }
        endTime = System.currentTimeMillis();
        difference = (endTime - startTime);
        System.out.println("time = " + difference + " ms, " + difference/(double) maxIter + " ms/oper");

        set = getUnion(a, b);
        printArray("getUnion (" + set.length + ")", set);
        startTime = System.currentTimeMillis();
        for (int i=0; i < maxIter; i++) {
            set = getUnion(a, b);
        }
        endTime = System.currentTimeMillis();
        difference = (endTime - startTime);
        System.out.println("time = " + difference + " ms, " + difference/(double) maxIter + " ms/oper");

        set = getDifference(a, b);
        printArray("getDifference (" + set.length + ")", set);
        startTime = System.currentTimeMillis();
        for (int i=0; i < maxIter; i++) {
            set = getDifference(a, b);
        }
        endTime = System.currentTimeMillis();
        difference = (endTime - startTime);
        System.out.println("time = " + difference + " ms, " + difference/(double) maxIter + " ms/oper");

        set = getIntersectionAndUnionCount(a, b);
        printArray("getIntersectionAndUnionCount (" + set[0] + ", " + set[1] + ")", set);
        startTime = System.currentTimeMillis();
        for (int i=0; i < maxIter; i++) {
            set = getIntersectionAndUnionCount(a, b);
        }
        endTime = System.currentTimeMillis();
        difference = (endTime - startTime);
        System.out.println("time = " + difference + " ms, " + difference/(double) maxIter + " ms/oper");
    }

    public static final void encode(final int[] array, final OutputStream os) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeInt(array.length);
        for (int i=0; i < array.length; i++) {
            dos.writeInt(array[i]);
        }
    }

    public static final byte[] encode(final int[] array) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        encode(array, bos);
        return bos.toByteArray();
    }

    public static final int[] decode(final InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);
        int length = dis.readInt();
        int[] result = new int[length];
        for (int i=0; i < result.length; i++) {
            result[i] = dis.readInt();
        }
        return result;
    }

    public static final int[] decode(final byte[] bytes) throws IOException {
        //System.out.println("bytes.length: " + bytes.length);
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        return decode(bis);
    }

}
