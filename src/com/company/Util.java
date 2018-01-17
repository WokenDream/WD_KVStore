package com.company;

/**
 * Created by tianqiliu on 2018-01-16.
 */

enum ReplacementPolicy {
    FIFO, LRU, LFU
}

class CacheNode {
    int freq = 0;
    String key;
    String val;

    public boolean equals(Object obj) {
        return (obj instanceof CacheNode) && ((CacheNode)obj).key.equals(this.key);
    }
}