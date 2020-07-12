package eu.unitn.disi.db.tool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolFactory {
    private static final ExecutorService WILDCARD_THREAD_POOL = Executors.newFixedThreadPool(8);
    private static final ExecutorService TABLE_COMPUTE_THREAD_POOL = Executors.newFixedThreadPool(8);
    private static final ExecutorService SEARCH_THREAD_POOL = Executors.newFixedThreadPool(1);

    public static ExecutorService getWildcardThreadPool() {
        return WILDCARD_THREAD_POOL;
    }

    public static ExecutorService getTableComputeThreadPool() {
        return TABLE_COMPUTE_THREAD_POOL;
    }

    public static ExecutorService getSearchThreadPool() {
        return SEARCH_THREAD_POOL;
    }

    public static void shutdownAll() {
        WILDCARD_THREAD_POOL.shutdown();
        TABLE_COMPUTE_THREAD_POOL.shutdown();
        SEARCH_THREAD_POOL.shutdown();
    }
}
