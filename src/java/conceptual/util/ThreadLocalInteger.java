package conceptual.util;

public class ThreadLocalInteger {

    private static final ThreadLocal<Integer> threadLocal = new ThreadLocal<Integer>();

    public static void set(Integer val) {
        threadLocal.set(val);
    }

    public static void unset() {
        threadLocal.remove();
    }

    public static Integer get() {
        return threadLocal.get();
    }
}
