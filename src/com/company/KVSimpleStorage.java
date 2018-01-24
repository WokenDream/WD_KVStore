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

    /**
     * Create/update the given key-pair on disk.
     * @param key given key
     * @param value value associated with key
     * @return true if this is an update; false if this is a create
     * @throws IOException
     */
    public boolean putKV(String key, String value) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IOException("invalid key " + key);
        }
        boolean updated = true;
        lock.lock();
        try {
            while (numOfReader > 0) {
                try {
                    noReaderCondition.await();
                } catch (InterruptedException e) {
                    // TODO: log
                }
            }
            File file = new File(getFilePath(key));
            if (value == null) {
                deleteFromStorage(key);
            } else if (file.exists()) {
                updatePair(file, key, value);
            } else {
                updated = false;
                createPair(file, key, value);
            }
        } catch (IOException ioe) {
            throw ioe;
        } finally {
            lock.unlock();
            return updated;
        }
    }

    /**
     * Get the value of the given key from disk.
     * @param key given key
     * @return associated value
     * @throws IOException
     */
    public String getKV(String key) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IOException("invalid key " + key);
        }
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
        } catch (IOException e) {
            // TODO: logging
            throw e;
        } finally {
            lock.lock();
            --numOfReader;
            if (numOfReader == 0) {
                noReaderCondition.signal();
            }
            lock.unlock();
        }

        return val;
    }

    /**
     * Check if the given key is in cache
     * @param key key to check
     * @return
     */
    public boolean inCache(String key) {
        return false;
    }

    /**
     * Check if the given key is on disk
     * @param key key to check
     * @return
     */
    public boolean inStorage(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        try {
            return getKV(key) != null;
        } catch (IOException e) {
            return false;
        }

    }

    /**
     * Clear storage
     */
    public void clearStorage() {
        lock.lock();
        File dir = new File(dbPath);
        File[] files = dir.listFiles();
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
        lock.unlock();
    }

    /**
     * Clear the content of cache
     */
    public void clearCache() {
        return;
    }

    /**
     * Delete the record associated with the given key.
     * Assumptions: key != null
     * @param key key to delete
     * @throws IOException
     */
    protected void deleteFromStorage(String key) throws IOException {
        File file = new File(getFilePath(key));
        if (file.exists()) {
            File tempFile = new File(dbPath + "temp.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
            BufferedReader reader = new BufferedReader(new FileReader(file));

            // read the old file into new file
            // if the old file already contains the given key, omit it
            int linesCopied = copyToFile(reader, writer, key);
            writer.close();
            reader.close();

            if (!file.delete()) {
                throw new IOException("could not delete the old file");
            }
            if (linesCopied == 0) {
                if (!tempFile.delete()) {
                    throw new IOException("could not delete the new file");
                }
            } else if (!tempFile.renameTo(file)) {
                throw new IOException("could not rename file");
            }

        }
    }

    /**
     * Compute the file path given a key.
     * Assumptions:
     * 1. key != null
     * @param key given key
     * @return file path
     */
    protected String getFilePath(String key) {
        // let the hashcode of the key be the name of the file
        StringBuilder sb = new StringBuilder(dbPath);
        sb.append(key.hashCode());
        sb.append(".txt");
        return sb.toString();
    }

    protected void createPair(File file, String key, String value) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(keyIndicator + key);
        writer.newLine();
        writer.write(valIndicator + value);
        writer.newLine();
        writer.close();
    }

    /**
     * Update the key-value pair in the given file.
     * If the pair does not exist, it creates the pair.
     * The updated pair is put in the beiginning of the file
     * for heuristic reason.
     * Assumptions:
     * 1. inputs are valid
     * @param file given file
     * @param key given key
     * @param value new value associated with the key
     * @throws IOException
     */
    protected void updatePair(File file, String key, String value) throws IOException {
        File tempFile = new File(dbPath + "temp.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
        BufferedReader reader = new BufferedReader(new FileReader(file));

        // put the given pair at the beginning of the file
        writer.write(keyIndicator + key);
        writer.newLine();
        writer.write(valIndicator + value);
        writer.newLine();

        // read the old file into new file
        // if the old file already contains the given key, omit it
        copyToFile(reader, writer, key);

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

    /**
     * Copy the content from one file to another, omitting the data associated with the given key.
     * @param reader reader for the origin file
     * @param writer writer for the destination file
     * @param keyToOmit key to be omitted
     * @return number of lines copied
     * @throws IOException
     */
    protected int copyToFile(BufferedReader reader, BufferedWriter writer, String keyToOmit) throws IOException {
        int linesCopied = 0;
        String str;
        while ((str = reader.readLine()) != null) {
            if (str.substring(afterIndicator).equals(keyToOmit)) {
                reader.readLine(); // skip the old value
                break;
            }

            ++linesCopied;
            writer.write(str);
            writer.newLine();
            writer.write(reader.readLine());
            writer.newLine();
        }
        while ((str = reader.readLine()) != null) {
            ++linesCopied;
            writer.write(str);
            writer.newLine();
            writer.write(reader.readLine());
            writer.newLine();
        }
        return linesCopied;
    }
}
