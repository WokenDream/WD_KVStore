package com.company;

import java.io.*;
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
    private final String keyIndicator = "k:";
    private final String valIndicator = "v:";
    private final int afterIndicator = 2;
    private KVCache cache;

    public KVStorage(String dbPath, int cacheCapacity, CacheStrategy strategy) throws InvalidPathException, IOException {

        if (dbPath == null || dbPath.isEmpty()) {
            // TODO: logging
            throw new InvalidPathException(dbPath, "Database path cannot be empty");
        } else if (dbPath.charAt(dbPath.length() - 1) != '/') {
            dbPath = dbPath + "/";
        }

        File dir = new File(dbPath);
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdir()) {
                // TODO: logging invalid dir path
                throw new InvalidPathException(dbPath, "Invalid database path");
            }
        }
        this.dbPath = dbPath;
        // TODO: create a class that is the same as this class except it has no cache
        cache = new KVCache(cacheCapacity, strategy);
    }

    public KVStorage(int cacheCapacity, CacheStrategy strategy) throws IOException {
        File dir = new File(dbPath);
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdir()) {
                // TODO: logging invalid dir path
                throw new InvalidPathException(dbPath, "Database creation failed");
            }
        }

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

    private String getFilePath(String key) {
        // let the hashcode of the key be the name of the file
        StringBuilder sb = new StringBuilder(dbPath);
        sb.append(key.hashCode());
        sb.append(".txt");
        return sb.toString();
    }

    private void appendToFile(File file, String key, String value) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        writer.write(keyIndicator + key);
        writer.newLine();
        writer.write(valIndicator + value);
        writer.newLine();
        writer.close();
    }

    private void updatePair(File file, String key, String value) throws IOException {
        File tempFile = new File(dbPath + "temp.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String str;
        while ((str = reader.readLine()) != null) {
            writer.write(str);
            writer.newLine();
            if (str.substring(afterIndicator).equals(key)) {
                writer.write(valIndicator + value);
                writer.newLine();
                reader.readLine();
                break;
            } else {
                writer.write(reader.readLine());
                writer.newLine();
            }
        }
        while ((str = reader.readLine()) != null) {
            writer.write(str);
            writer.newLine();
            writer.write(reader.readLine());
            writer.newLine();
        }
        writer.close();
        reader.close();
        if (!file.delete()) {
            // TODO: logging
            throw new IOException("could not delete the old file");
        }
        if (!tempFile.renameTo(file)) {
            // TODO: logging
            throw new IOException("could not rename file");
        }
    }
}
