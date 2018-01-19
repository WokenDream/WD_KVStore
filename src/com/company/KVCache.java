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

    private int remainSize;
    private int cacheCapacity;
    private final CacheStrategy replacePolicy;
    private HashMap<String, String> cache; // key is the "key", value is the "value"

    // maintain the order for cache replacement policy
    // LFU: least is at head; LRU: least is at head
    // FIFO: put to tail, pop from head; None: same as FIFO
    private LinkedList<CacheNode> list = new LinkedList<>();

    private ReentrantLock lock = new ReentrantLock();
    private int numOfReader = 0;
    private Condition noReaderCondition = lock.newCondition();


    public KVCache(int cacheCapacity, CacheStrategy replacePolicy) {
        // cache setup
        if (cacheCapacity < 1) {
            cacheCapacity = 100;
        }
        if (replacePolicy == CacheStrategy.None) {
            replacePolicy = CacheStrategy.FIFO;
        }
        this.remainSize = cacheCapacity;
        this.cacheCapacity = cacheCapacity;
        this.replacePolicy = replacePolicy;
        this.cache = new HashMap<>();
    }

    /**
     * Put the key-value pair into cache, eviction may take place
     * @param key key of the data
     * @param value value of the data
     */
    public void putKV(String key, String value) {
        lock.lock();
        if (value.length() > cacheCapacity) {
            lock.unlock();
            return;
        }
        while (numOfReader > 0) {
            try {
                noReaderCondition.await();
            } catch (InterruptedException e) {
                // TODO: log
            }
        }

        String val = cache.get(key);
        if (val == null) {
            if (remainSize < value.length()) {
                evict(value.length());
            }
            insert(key, value);
        } else {
            int oldLen = val.length(), newLen = value.length();
            if (remainSize + oldLen < newLen) {
                evict(newLen - oldLen);
            }
            update(key, value, newLen - oldLen);
        }
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

        String val = cache.get(key);
        if (val != null && replacePolicy != CacheStrategy.FIFO) {
            lock.lock();
            updateOrderList(key);
            lock.unlock();
        }

        lock.lock();
        --numOfReader;
        if (numOfReader == 0) {
            noReaderCondition.signal();
        }
        lock.unlock();

        return val;
    }

    /**
     * Ensure minimum capacity of the cache. The resulting capacity will be:
     * min(newCacheCapacity, old cache capacity)
     * @param newCacheCapacity
     */
    public void ensureCacheCapacity(int newCacheCapacity) {
        if (cacheCapacity < newCacheCapacity) {
            cacheCapacity = newCacheCapacity;
        }
    }

    /**
     * Evict the cache so that the remaining size of the cache
     * is large enough to support the required size.
     * @param requiredSize size required
     */
    private void evict(int requiredSize) {
        while (remainSize < requiredSize) {
            CacheNode node = list.poll();
            String removedVal = cache.remove(node.key);
            remainSize += removedVal.length();
        }
    }

    /**
     * Insert the given key-value pair into cache.
     * Assumptions:
     * 1. the given pair is a new pair
     * 2. the cache is big enough
     * @param key the key of pair
     * @param value the value of pair
     */
    private void insert(String key, String value) {
        CacheNode node = new CacheNode(key);

        if (replacePolicy == CacheStrategy.LFU) {
            list.addFirst(node);
        } else {
            list.addLast(node);
        }

        cache.put(key, value);
        remainSize -= value.length();
    }

    /**
     * Update the pair with the given key
     * Assumptions:
     * 1. the pair already exists in the cache
     * 2. the cache is big enough
     * @param key key of the pair
     * @param value new value of the pair
     * @param changeInSize the total change of the cacheSize = newLen - oldLen of the updated element
     */
    private void update(String key, String value, int changeInSize) {
        CacheNode node = new CacheNode(key);

        if (replacePolicy == CacheStrategy.LRU) {
            list.remove(node);
            list.add(node);
        } else if (replacePolicy == CacheStrategy.LFU) {
            updateLFUList(node);
        }
        cache.put(key, value);
        remainSize -= changeInSize;

    }

    /**
     * Update the position and frequency of the node with the given key
     * Assumptions:
     * 1. the given node is already in the list
     * @param node the node to be updated
     */
    private void updateLFUList(CacheNode node) {
        int i = list.indexOf(node);
        node = list.remove(i);
        ++(node.freq);
        insertIntoLFUList(node);
    }

    /**
     * A helper function for inserting cache node into LFU list in the correct order
     * @param node the node to be updated in the order list
     */
    private void insertIntoLFUList(CacheNode node) {
        int len = list.size();
        int i = 0;
        while (i < len) {
            if (node.freq <= list.get(i).freq) {
                break;
            }
            ++i;
        }
        list.add(i, node);
    }

    /**
     * Update the position of the cache node associated with the given key
     * @param key key of the target node
     * Assumptions:
     * 1. key != null
     * 2. the target node is already in the list
     */
    private void updateOrderList(String key) {
        CacheNode node = new CacheNode(key);
        switch (replacePolicy) {
            case LRU:
                int i = list.indexOf(node);
                node = list.remove(i);
                list.addLast(node);
            case LFU:
                updateLFUList(node);
        }
    }
}
