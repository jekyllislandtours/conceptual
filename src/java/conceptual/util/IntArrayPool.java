package conceptual.util;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;

import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Arrays;

public class IntArrayPool {

    private GenericObjectPool intArrayPool;

    public IntArrayPool() {
        intArrayPool = createObjectPool();
    }

    private GenericObjectPool<int[]> createObjectPool() {
        PooledObjectFactory<int[]> intArrayFactory = new PooledObjectFactory<int[]>() {
            // Reinitialize an instance to be returned by the pool.
            public void activateObject(PooledObject<int[]> obj) {

            }

            // Destroys an instance no longer needed by the pool.
            public void destroyObject(PooledObject<int[]> obj) {}

            // Creates an instance that can be served by the pool.
            public PooledObject<int[]> makeObject() {
                return new DefaultPooledObject<>(new int[ThreadLocalInteger.get()]);
            }

            // Uninitialize an instance to be returned to the idle object pool.
            public void passivateObject(PooledObject<int[]> obj) {
                int[] array = obj.getObject();
                Arrays.fill(array, 0);
            }

            // Ensures that the instance is safe to be returned by the pool.
            public boolean validateObject(PooledObject<int[]> obj)  {
                return obj.getObject().length == ThreadLocalInteger.get();
            }
        };

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(10);
        config.setMaxTotal(20);
        config.setMaxWaitMillis(1000);
        return new GenericObjectPool<>(intArrayFactory, config);
    }

    public int[] borrowArray(int size) throws Exception {
        ThreadLocalInteger.set(size);
        return (int[]) intArrayPool.borrowObject();
    }

    public void returnArray(int[] array) {
        intArrayPool.returnObject(array);
    }
}
