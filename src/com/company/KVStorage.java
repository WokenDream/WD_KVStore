package com.company;

import java.io.*;
import java.nio.file.InvalidPathException;

/**
 * Created by tianqiliu on 2018-01-18.
 */
public class KVStorage extends KVSimpleStorage {

    private KVCache cache;

    public KVStorage(String dbPath, int cacheCapacity, CacheStrategy strategy) throws InvalidPathException, IOException {
        super(dbPath);
        cache = new KVCache(cacheCapacity, strategy);
    }

    public KVStorage(int cacheCapacity, CacheStrategy strategy) throws IOException {
        super();
        cache = new KVCache(cacheCapacity, strategy);
    }

    public void putKV(String key, String value) throws IOException {
        lock.lock();
        while (numOfReader > 0) {
            try {
                noReaderCondition.await();
            } catch (InterruptedException e) {
                // TODO: log
            }
        }
        cache.putKV(key, value);

        File file = new File(getFilePath(key));
        if (file.exists()) {
            updatePair(file, key, value);
        } else {
            appendToFile(file, key, value);
        }
        lock.unlock();
    }

    public String getKV(String key) {
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
                        val = reader.readLine().substring(afterIndicator);
                        break;
                    }
                    reader.readLine();
                }
                reader.close();
            } catch (FileNotFoundException e) {
                // TODO: logging
                // invalid key
                System.out.println(e.getMessage());
            } catch (IOException e) {
                // TODO: logging
                System.out.println(e.getMessage());
            }

            lock.lock();
            if (val != null) {
                cache.putKV(key, val);
            }

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
