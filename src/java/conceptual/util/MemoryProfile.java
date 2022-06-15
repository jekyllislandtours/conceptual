package conceptual.util;

public final class MemoryProfile {

    long freeMemory;
    long totalMemory;
    long maxMemory;

    private MemoryProfile() {}

    public long getFreeMemory() {
        return freeMemory;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public static MemoryProfile diff(MemoryProfile profileA, MemoryProfile profileB) {
        MemoryProfile diff = new MemoryProfile();
        diff.freeMemory = profileA.freeMemory - profileB.freeMemory;
        diff.totalMemory = profileA.totalMemory - profileB.totalMemory;
        diff.maxMemory = profileA.maxMemory - profileB.maxMemory;
        return diff;
    }

    public static MemoryProfile measure() {
        Runtime runtime = Runtime.getRuntime();
        MemoryProfile result = new MemoryProfile();
        result.freeMemory = runtime.freeMemory();
        result.totalMemory = runtime.totalMemory();
        result.maxMemory = runtime.maxMemory();
        return result;
    }

    public MemoryProfile diff(MemoryProfile profile) {
        return diff(this, profile);
    }

    public String toString() {
        return ("memory free: " + freeMemory/1024/1024 +
                "MB, total memory:" + totalMemory/1024/1024 +
                "MB, max memory:" + maxMemory/1024/1024 + "MB");
    }
}
