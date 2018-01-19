package com.company;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.InvalidPathException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tianqiliu on 2018-01-18.
 */
public class KVStorage {

    private ReentrantLock lock = new ReentrantLock();
    private int numOfReader = 0;
    private Condition noReaderCondition = lock.newCondition();

    private String dbPath = "./db/";
    private KVCache cache;

    public KVStorage(String dbPath, int cacheCapacity, CacheStrategy strategy) throws InvalidPathException {

        if (dbPath == null || dbPath.isEmpty()) {
            // TODO: logging
            throw new InvalidPathException(dbPath, "Database path cannot be empty");
        } else if (dbPath.charAt(dbPath.length() - 1) != '/') {
            dbPath = dbPath + "/";
        }

        File dir = new File(dbPath);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                // TODO: logging invalid dir path
                throw new InvalidPathException(dbPath, "Invalid database path");
            }
        }
        // TODO: create a class that is the same as this class except it has no cache
        cache = new KVCache(cacheCapacity, strategy);
    }

    public KVStorage(int cacheCapacity, CacheStrategy strategy) {
        File dir = new File(dbPath);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                // TODO: logging invalid dir path
                throw new InvalidPathException(dbPath, "Database creation failed");
            }
        }
        cache = new KVCache(cacheCapacity, strategy);
    }

    public void putKV(String key, String value) {
        lock.lock();
        while (numOfReader > 0) {
            try {
                noReaderCondition.await();
            } catch (InterruptedException e) {
                // TODO: log
            }
        }
        cache.putKV(key, value);
        StringBuilder sb = new StringBuilder(dbPath);
        sb.append(key.hashCode());

        String filePath = sb.toString();
        File file = new File(filePath);
        if (file.exists()) {
            // TODO: if KV pair already exists in this file => {update}, else => {append to file}
        } else {
            // TODO: create a new file with the KV pair
        }
        lock.unlock();
    }

    public String getKV(String key) {
        lock.lock();
        ++numOfReader;
        lock.unlock();

        String val = cache.getKV(key);
        if (val == null) {
            // TODO: read from disk and put in cache, then lock so multiple loading from disk is possible
            // TODO: may need to check if there is IO exception if the file is already open in read mode
//            lock.lock();
//            cache.putKV(key, val);
        } else {
            lock.lock();
            cache.updateOrderList(key);
        }

        --numOfReader;
        if (numOfReader == 0) {
            noReaderCondition.signal();
        }
        lock.unlock();

        return val;
    }
}
