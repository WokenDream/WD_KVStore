package com.company;

import com.sun.javaws.exceptions.InvalidArgumentException;

import java.io.*;
import java.nio.file.InvalidPathException;

/**
 * Created by tianqiliu on 2018-01-18.
 */
public class KVStorage extends KVSimpleStorage {

    private KVCache cache;

    public KVStorage(String dbPath, int cacheCapacity, IKVServer.CacheStrategy strategy) throws InvalidPathException, IOException {
        super(dbPath);
        cache = new KVCache(cacheCapacity, strategy);
    }

    public KVStorage(int cacheCapacity, IKVServer.CacheStrategy strategy) throws IOException {
        super();
        cache = new KVCache(cacheCapacity, strategy);
    }

    /**
     * Create/update given key-value pair to disk and cache.
     * @param key given key
     * @param value value associated with key
     * @return status of result
     * @throws IOException
     */
    public KVStorageResult putKV(String key, String value) throws IOException {
        if (key == null || key.isEmpty() || value == null || value.isEmpty()) {
            throw new IOException("invalid arguments key - " + key + " value - " + value);
        }
        KVStorageResult result = new KVStorageResult();
        lock.lock();
        try {
            while (numOfReader > 0) {
                try {
                    noReaderCondition.await();
                } catch (InterruptedException e) {
                    // TODO: log
                }
            }
            if (value.equals("null")) {
                if (deleteFromStorage(key)) {
                    result.setResult(KVStorageResult.ResultType.DELETE_SUCCESS);
                } else {
                    result.setResult(KVStorageResult.ResultType.DELETE_ERROR);
                }
            } else {
                cache.putKV(key, value);
                File file = new File(getFilePath(key));
                if (file.exists()) {
                    if (updatePair(file, key, value)) {
                        result.setResult(KVStorageResult.ResultType.PUT_UPDATE_SUCCESS);
                    } else {
                        result.setResult(KVStorageResult.ResultType.PUT_UPDATE_ERROR);
                    }
                } else {
                    try {
                        createPair(file, key, value);
                        result.setResult(KVStorageResult.ResultType.PUT_SUCCESS);
                    } catch (IOException ioe) {
                        result.setResult(KVStorageResult.ResultType.PUT_ERROR);
                    }

                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            lock.unlock();
        }
        return result;
    }

    /**
     * Return the value of the associated key from cache/disk.
     * @param key given key
     * @return associated result object
     * @throws IOException
     */
    public KVStorageResult getKV(String key) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IOException("invalid key " + key);
        }
        KVStorageResult result = new KVStorageResult(KVStorageResult.ResultType.GET_ERROR);
        lock.lock();
        ++numOfReader;
        lock.unlock();

        String val = cache.getKV(key);
        if (val == null) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(getFilePath(key)));
                String str;
                while ((str = reader.readLine()) != null) {
                    if (str.substring(afterIndicator).equals(key)) {
                        result.setResult(KVStorageResult.ResultType.GET_SUCCESS);
                        result.setValue(reader.readLine().substring(afterIndicator));
                        break;
                    }
                    reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                // TODO: logging
                // invalid key
                System.out.println(e.getLocalizedMessage());
            } finally {
                lock.lock();
                if (val != null) {
                    cache.putKV(key, val);
                }
                --numOfReader;
                if (numOfReader == 0) {
                    noReaderCondition.signal();
                }
                lock.unlock();
            }

        } else {
            result.setResult(KVStorageResult.ResultType.GET_SUCCESS);
            result.setValue(val);
            lock.lock();
            cache.updateOrderList(key);
            --numOfReader;
            if (numOfReader == 0) {
                noReaderCondition.signal();
            }
            lock.unlock();
        }

        return result;
    }

    /**
     * Check if the given key is stored in cache of this storage object
     * @param key key to check
     * @return
     */
    public boolean inCache(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        lock.lock();
        ++numOfReader;
        lock.unlock();

        boolean in = cache.inCache(key);

        lock.lock();
        --numOfReader;
        if (numOfReader == 0) {
            noReaderCondition.signal();
        }
        lock.unlock();
        return in;
    }

    /**
     * Check if the given key is on disk
     * @param key key to check
     * @return
     */
    public boolean inStorage(String key) {
        return inCache(key) || super.inStorage(key);
    }

    /**
     * Clear the cache associated with this storage object
     */
    public void clearCache() {
        lock.lock();
        cache.clearCache();
        lock.unlock();
    }

    /**
     * Clear storage
     */
    public void clearStorage() {
        lock.lock();
        cache.clearCache();
        File dir = new File(dbPath);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file: files) {
                if (!file.delete()) {
                    // TODO: logging
                    System.out.println("Failed to delete " + file);
                }
            }
            if(!dir.delete()) {
                // TODO: logging
                System.out.println("Failed to delete " + dir);
            }
        }

        lock.unlock();
    }

    /**
     * Delete the key-value pair from cache and disk.
     * @param key key to delete
     * @return
     * @throws IOException
     */
    protected boolean deleteFromStorage(String key) throws IOException {
        cache.deleteFromCache(key);
        return super.deleteFromStorage(key);
    }
}
