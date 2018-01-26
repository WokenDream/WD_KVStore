package com.company;

/**
 * Created by tianqiliu on 2018-01-16.
 */
interface IKVServer {
    enum CacheStrategy {
        None, FIFO, LRU, LFU
    }
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

class KVStorageResult {

    public enum ResultType {
        PUT_SUCCESS,
        PUT_ERROR,
        PUT_UPDATE_SUCCESS,
        PUT_UPDATE_ERROR,
        DELETE_SUCCESS,
        DELETE_ERROR,
        GET_SUCCESS,
        GET_ERROR
    }

    private ResultType result;
    private String value;

    public KVStorageResult() {
        result = null;
        value = null;
    }

    public KVStorageResult (ResultType result) {
        this.result = result;
    }

    public KVStorageResult (ResultType result, String value) {
        this.result = result;
        this.value = value;
    }

    public void setResult(ResultType result) {
        this.result = result;
    }

    public ResultType getResult() {
        return this.result;
    }
    public void setValue(String value) {
        this.value = value;
    }
    public String getValue() {
        return this.value;
    }
}
