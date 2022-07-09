package conceptual.util;

interface IntegerRef {
    int get();

    void set(int v);

    static class Value implements IntegerRef {
        int value;

        public int get() {
            return value;
        }

        public void set(int v) {
            value = v;
        }
    }
}
