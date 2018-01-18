package com.company;

/**
 * Created by tianqiliu on 2018-01-16.
 */

enum ReplacementPolicy {
    FIFO, LRU, LFU
}

class CacheNode {
    String key;
    int freq = 1;

    public CacheNode(String key) {
        this.key = key;
    }

    public CacheNode(String key, int freq) {
        this.key = key;
        this.freq = freq;
    }

    public boolean equals(Object obj) {
        return (obj instanceof CacheNode) && ((CacheNode)obj).key.equals(this.key);
    }
}