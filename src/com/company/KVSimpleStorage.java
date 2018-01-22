package com.company;

import java.io.*;
import java.nio.file.InvalidPathException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by tianqiliu on 2018-01-22.
 */
public class KVSimpleStorage {
    protected ReentrantLock lock = new ReentrantLock();
    protected int numOfReader = 0;
    protected Condition noReaderCondition = lock.newCondition();

    protected String dbPath = "./db/";
    protected final String keyIndicator = "k:";
    protected final String valIndicator = "v:";
    protected final int afterIndicator = 2;

    public KVSimpleStorage(String dbPath) throws InvalidPathException, IOException {

        if (dbPath == null || dbPath.isEmpty()) {
            throw new InvalidPathException(dbPath, "Database path cannot be empty");
        } else if (dbPath.charAt(dbPath.length() - 1) != '/') {
            dbPath = dbPath + "/";
        }

        File dir = new File(dbPath);
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdir()) {
                throw new InvalidPathException(dbPath, "Could not make database at the given path");
            }
        }
        this.dbPath = dbPath;
    }

    public KVSimpleStorage() throws IOException {
        File dir = new File(dbPath);
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdir()) {
                throw new InvalidPathException(dbPath, "Database creation failed");
            }
        }
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

        String val = null;
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
        --numOfReader;
        if (numOfReader == 0) {
            noReaderCondition.signal();
        }
        lock.unlock();

        return val;
    }

    protected String getFilePath(String key) {
        // let the hashcode of the key be the name of the file
        StringBuilder sb = new StringBuilder(dbPath);
        sb.append(key.hashCode());
        sb.append(".txt");
        return sb.toString();
    }

    protected void appendToFile(File file, String key, String value) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        writer.write(keyIndicator + key);
        writer.newLine();
        writer.write(valIndicator + value);
        writer.newLine();
        writer.close();
    }

    protected void updatePair(File file, String key, String value) throws IOException {
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
