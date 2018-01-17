package com.company;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tianqiliu on 2018-01-16.
 */

/**
 * This is a thread-safe cache class allowing specify the cache size and replacement policy
 * The cache instance allows read from multiple threads if the cache is not being updated.
 */
public class KVCache {

    private int cacheSize;
    private ReplacementPolicy replacePolicy;
    private HashMap<String, LinkedList<CacheNode>> cache;

    private ReentrantLock lock = new ReentrantLock();
    private int numOfReader = 0;
    private Condition noReaderCondition = lock.newCondition();


    public KVCache(int cacheSize, ReplacementPolicy replacePolicy) {
        // cache setup
        this.cacheSize = cacheSize;
        this.replacePolicy = replacePolicy;
        cache = new HashMap<>();
    }

    /**
     * Put the key-value pair into cache, eviction may take place
     * @param key key of the data
     * @param value value of the data
     */
    public void putKV(String key, String value) {
        lock.lock();
        while (numOfReader > 0) {
            try {
                noReaderCondition.await();
            } catch (InterruptedException e) {
                // TODO: log
            }
        }

        // TODO: if space is not enough => {evict}; then => {insert into cache}
        lock.unlock();
    }

    /**
     * Get the value associated with the given key
     * @param key key of the data
     * @return value associated with the key
     */
    public String getKV(String key) {
        lock.lock();
        ++numOfReader;
        lock.unlock();

        String val = null;
        // TODO: worker code
        LinkedList<CacheNode> list = cache.get(key);
        if (list != null) {

        }

        lock.lock();
        --numOfReader;
        if (numOfReader == 0) {
            noReaderCondition.signal();
        }
        lock.unlock();

        return val;
    }
}
