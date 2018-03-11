package com.company;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by tianqiliu on 2018-02-24.
 */
public class ECSNode implements IECSNode, Serializable {

    private static String HASH_ALGO = "MD5";

    public enum Action {
        None, Start, Stop, Kill
    }

    private String name;
    private String ipAddress;
    private int port;
    private String[] hashRange; // (predecessor, me]
    public TreeMap<String, IECSNode> hashRing = new TreeMap<>(); // (hash, ecsnode)

    private int cacheSize;
    private String cacheStrategy;

    // stated vars
    public boolean inUse = false; // can only be touched by ECS
    public boolean connected = false; // can only be touched by KVServer
    public boolean started = false; // can only be touched by KVServer
    public boolean killed = false; // can only be touched by KVServer
    public Action todo = Action.None;

    public ECSNode(String name, String ipAddress, int port) {
        initNode(name, ipAddress, port);
    }

    //----------------IECSNode implementation----------------//
    public String getNodeName() {
        return name;
    }

    public String getNodeHost() {
        return ipAddress;
    }

    public int getNodePort() {
        return port;
    }

    public String[] getNodeHashRange() {
        return hashRange;
    }

    //------------------Custom Methods------------------//
    private void initNode(String name, String ipAddress, int port) {
        this.name = name;
        this.ipAddress = ipAddress;
        this.port = port;
        hashRange = new String[2];
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGO);
            byte[] bytes = messageDigest.digest((ipAddress + ":" + port).getBytes());
            hashRange[1] = DatatypeConverter.printHexBinary(bytes);
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public static ECSNode fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis);
        ECSNode node = (ECSNode) in.readObject();
        bis.close();
        in.close();
        return node;
    }

    public void setCacheSize(int size) {
        this.cacheSize = size;
    }

    public void setCacheStrategy(String strategy) {
        this.cacheStrategy = strategy;
    }

    public void setCache(String cacheStrategy, int cacheSize) {
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
    }

    public String getNodeHash() {
        return hashRange[1];
    }

    public void setNodeHashLowRange(String predecessorHash) {
        hashRange[0] = predecessorHash;
    }

    public int getNodeCacheSize() {
        return cacheSize;
    }

    public String getNodeCacheStrategy() {
        return cacheStrategy == null ? "" : cacheStrategy;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(this);
        out.flush();
        byte[] bytes = bos.toByteArray();
        bos.close();
        out.close();
        return bytes;
    }

    public void setNodeName(String newName) {
        name = newName;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public boolean isKeyInRange(String key) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGO);
            byte[] bytes = messageDigest.digest(key.getBytes());
            String hash = DatatypeConverter.printHexBinary(bytes);
            if (hashRange[0].compareTo(hash) < 0 && hash.compareTo(hashRange[1]) <= 0) {
                return true;
            } else {
                String smallestHash = hashRing.firstKey();
                String largestHash = hashRing.lastKey();
                if (smallestHash.equals(hashRange[1]) && (hash.compareTo(smallestHash) <= 0 || hash.compareTo(largestHash) > 0)) {
                    return true;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getLocalizedMessage());
            return false;
        }
        return false;

    }

    public TreeMap<String, IECSNode> getHashRing() {
        return hashRing;
    }

    public IECSNode getSuccessor() {
        Map.Entry<String, IECSNode> successorEntry = hashRing.higherEntry(hashRange[1]);
        if (successorEntry == null) {
            successorEntry = hashRing.firstEntry();
        }
        return successorEntry.getValue();
    }
}
