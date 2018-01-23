package com.company;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tianqiliu on 2018-01-16.
 */

/**
 * This is a non thread-safe cache class
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
     * 1. If the key does not exist, create a new record
     * 2. otherwise update the existing record
     * @param key key of the data
     * @param value value of the data
     */
    public void putKV(String key, String value) {
        if (value.length() > cacheCapacity) {
            return;
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
    }

    /**
     * Get the value associated with the given key
     * @param key key of the data
     * @return value associated with the key
     */
    public String getKV(String key) {
        return cache.get(key);
    }

    /**
     * Update the position of the cache node associated with the given key
     * This function can only be called if getKV(String key) != null
     * @param key key of the target node
     * Assumptions:
     * 1. key != null
     * 2. the target node is already in the list
     */
    public void updateOrderList(String key) {
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
     * Check if the pair with given key is in cache
     * @param key key to check
     * @return
     */
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    /**
     * Remove everything in the cache
     */
    public void clearCache() {
        list.clear();
        cache.clear();
        remainSize = cacheCapacity;
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

}
