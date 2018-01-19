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
            throw new InvalidPathException(dbPath, "Database path cannot be empty");
        } else if (dbPath.charAt(dbPath.length() - 1) != '/') {
            throw new InvalidPathException(dbPath, "Database path must end with '/'");
        }
        File dir = new File(dbPath);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                // TODO: logging invalid dir path
                throw new InvalidPathException(dbPath, "Invalid database path");
            }
        }
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

        }
        lock.unlock();
    }

    public String getKV(String key) {
        lock.lock();
        ++numOfReader;
        lock.unlock();

        String val = cache.getKV(key);
        if (val == null) {
            // TODO: look up from disk
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
