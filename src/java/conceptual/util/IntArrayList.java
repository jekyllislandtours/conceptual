package conceptual.util;

import java.util.Arrays;

import clojure.lang.ISeq;
import clojure.lang.Cons;
import clojure.lang.PersistentList;
import clojure.lang.Util;
import clojure.lang.Counted;
import clojure.lang.Seqable;
import clojure.lang.Indexed;

import java.util.Arrays;

/**
 * A custom ArrayList implementation that uses a primitive int array
 * as its backing store. This is more memory-efficient than
 * java.util.ArrayList<Integer> as it avoids autoboxing and object overhead.
 */
public class IntArrayList implements Counted, Seqable, Indexed {

    // The default capacity for the array if not specified.
    private static final int DEFAULT_CAPACITY = 10;

    // The primitive integer array to store the elements.
    private int[] data;

    // The current number of elements in the list.
    private int size;

    /**
     * Constructs a new IntArrayList with the default capacity.
     */
    public IntArrayList() {
        this.data = new int[DEFAULT_CAPACITY];
        this.size = 0;
    }

    /**
     * Constructs a new IntArrayList with a specified initial capacity.
     *
     * @param initialCapacity The initial capacity of the list.
     * @throws IllegalArgumentException if the initial capacity is negative.
     */
    public IntArrayList(final int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity cannot be negative: " + initialCapacity);
        }
        this.data = new int[initialCapacity];
        this.size = 0;
    }

    /**
     * Adds a new element to the end of the list.
     *
     * @param element The element to be added.
     */
    public void add(final int element) {
        // Ensure there is enough capacity to add the new element
        ensureCapacity(size + 1);
        data[size++] = element;
    }

    /**
     * Adds a new element at a specific index.
     *
     * @param index   The index at which to add the element.
     * @param element The element to be added.
     * @throws IndexOutOfBoundsException if the index is out of bounds.
     */
    public void add(final int index, final int element) {
        // Validate the index
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }

        // Ensure there is enough capacity to add the new element
        ensureCapacity(size + 1);

        // Shift elements to the right to make space for the new element
        for (int i = size - 1; i >= index; i--) {
            data[i + 1] = data[i];
        }

        data[index] = element;
        size++;
    }

    /**
     * Adds an Number's primitive int value to the end of the list, only if it is not null.
     *
     * @param element The Number object to be added.
     */
    public void add(final Number element) {
        if (element != null) {
            // Autoboxing will convert the Integer object to a primitive int
            add(element.intValue());
        }
    }

    /**
     * Adds all elements from the specified integer array to the end of this list.
     *
     * @param elements The array of integers to be added.
     */
    public void addAll(final int[] elements) {
        if (elements == null || elements.length == 0) {
            return;
        }

        final int numToAdd = elements.length;
        ensureCapacity(size + numToAdd);
        System.arraycopy(elements, 0, data, size, numToAdd);
        size += numToAdd;
    }

    public void add(final IntArrayList l) {
        if (null != l) {
            addAll(l.data);
        }
    }


    public void addAll(Seqable coll) {
        if (null == coll) {
            return;
        }
        ISeq xs = coll.seq();
        while (null!=xs) {
            add((Number) xs.first());
            xs = xs.next();
        }
    }



    /**
     * Returns the element at the specified index.
     *
     * @param index The index of the element to return.
     * @return The element at the specified index.
     * @throws IndexOutOfBoundsException if the index is out of bounds.
     */
    public int get(final int index) {
        // Validate the index
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return data[index];
    }

    /**
     * Replaces the element at the specified index with the new element.
     *
     * @param index   The index of the element to replace.
     * @param element The new element.
     * @return The old element that was replaced.
     * @throws IndexOutOfBoundsException if the index is out of bounds.
     */
    public int set(final int index, final int element) {
        // Validate the index
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        final int oldValue = data[index];
        data[index] = element;
        return oldValue;
    }

    /**
     * Removes the element at the specified index.
     *
     * @param index The index of the element to remove.
     * @return The element that was removed.
     * @throws IndexOutOfBoundsException if the index is out of bounds.
     */
    public int remove(final int index) {
        // Validate the index
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        final int removedValue = data[index];

        // Shift elements to the left to fill the gap
        for (int i = index; i < size - 1; i++) {
            data[i] = data[i + 1];
        }

        // Decrement the size
        size--;

        return removedValue;
    }

    /**
     * Returns the number of elements in the list.
     *
     * @return The number of elements.
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if the list contains no elements.
     *
     * @return True if the list is empty, false otherwise.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns a string representation of the list.
     *
     * @return A string representation of the list.
     */
    @Override
    public String toString() {
        if (size == 0) {
            return "[]";
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < size; i++) {
            sb.append(data[i]);
            if (i < size - 1) {
                sb.append(" ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns a sequence (ISeq) representation of the list.
     * This method is the entry point for Clojure's sequence abstraction.
     *
     * @return An ISeq instance representing the list, or null if empty.
     */
    public ISeq seq() {
        if (size == 0) {
            return null;
        }
        return new IntArrayListSeq(0, this);
    }

    /**
     * Returns the number of elements in the list. This method is part of the
     * clojure.lang.Counted interface.
     *
     * @return The number of elements in the list.
     */
    @Override
    public int count() {
        return size;
    }

    /**
     * Returns the value at the specified index. This method is part of the
     * clojure.lang.Indexed interface.
     *
     * @param index The index to retrieve the value from.
     * @return The value at the specified index, as an Object.
     * @throws IndexOutOfBoundsException if the index is out of bounds.
     */
    @Override
    public Object nth(final int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return Integer.valueOf(data[index]);
    }

    /**
     * Returns the value at the specified index, or a default value if the index
     * is out of bounds. This method is part of the clojure.lang.Indexed interface.
     *
     * @param index The index to retrieve the value from.
     * @param notFound The value to return if the index is out of bounds.
     * @return The value at the specified index, or notFound if out of bounds.
     */
    @Override
    public Object nth(final int index, final Object notFound) {
        if (index < 0 || index >= size) {
            return notFound;
        }
        return Integer.valueOf(data[index]);
    }


    /**
     * Returns a COPY of the internal backing store
     */
    public int[] toIntArray() {
        return Arrays.copyOf(data, size);
    }

    private static int dedupe(int[] xs) {
        // degenerate edge cases
        if (xs.length == 0 || xs.length == 1) {
            return xs.length;
        }

        // Initialize two pointers:
        // 'i' iterates through the array to find unique elements.
        // 'j' points to the next position where a unique element should be placed.
        int j = 0;

        // Iterate through the array starting from the second element
        for (int i = 1; i < xs.length; i++) {
            // If the current element (xs[i]) is different from the element at 'j' (xs[j]),
            // it means we've found a new unique element.
            if (xs[i] != xs[j]) {
                j++; // Move 'j' to the next position
                xs[j] = xs[i]; // Place the unique element at xs[j]
            }
        }

        // The number of unique elements is j + 1 (since j is 0-indexed)
        return j + 1;
    }


    public int[] toSortedIntSet() {
        int[] xs = toIntArray();
        Arrays.sort(xs);
        int idx = dedupe(xs);
        return Arrays.copyOf(xs, idx);
    }


    public static IntArrayList fromSeqable(Seqable coll) {
        final IntArrayList l = new IntArrayList();
        l.addAll(coll);
        return l;
    }

    public static int[] sortedIntSet(Seqable coll) {
        return fromSeqable(coll).toSortedIntSet();
    }

    /**
     * Increases the capacity of the backing array if it is full.
     * The new capacity is the old capacity plus half of the old capacity.
     *
     * @param requiredCapacity The minimum required capacity.
     */
    private void ensureCapacity(final int requiredCapacity) {
        // Only resize if the current capacity is less than the required capacity
        if (requiredCapacity > data.length) {
            // Calculate new capacity, ensuring it's at least the required capacity
            long newCapacity = (long)data.length + (data.length >> 1);
            if (newCapacity < requiredCapacity) {
                newCapacity = requiredCapacity;
            }

            if (newCapacity > Integer.MAX_VALUE) {
                throw new IllegalStateException("Capacity would exceed Integer.MAX_VALUE");
            }

            final int[] newData = new int[(int)newCapacity];
            // Copy all elements from the old array to the new one
            System.arraycopy(data, 0, newData, 0, size);
            this.data = newData;
        }
    }

    /**
     * A private nested class that implements the Clojure ISeq interface
     * to provide a sequence view of the IntArrayList.
     */
    private static class IntArrayListSeq implements ISeq {
        private final int index;
        private final IntArrayList list;

        IntArrayListSeq(final int index, final IntArrayList list) {
            this.index = index;
            this.list = list;
        }

        @Override
        public Object first() {
            if (index < list.size) {
                return Integer.valueOf(list.data[index]);
            }
            return null;
        }

        @Override
        public ISeq next() {
            if (index + 1 < list.size) {
                return new IntArrayListSeq(index + 1, list);
            }
            return null;
        }

        @Override
        public ISeq more() {
            if (index + 1 < list.size) {
                return new IntArrayListSeq(index + 1, list);
            }
            return PersistentList.EMPTY;
        }


        @Override
        public ISeq empty() {
            return PersistentList.EMPTY;
        }

        @Override
        public ISeq cons(final Object o) {
            // Prepending an element to the sequence view
            return new Cons(o, this);
        }

        @Override
        public int count() {
            return list.size - index;
        }

        @Override
        public boolean equiv(final Object o) {
            return Util.equiv(this, o);
        }

        public ISeq seq() {
            if (list.size == 0) {
                return PersistentList.EMPTY;
            }
            return this;
        }
    }
}
