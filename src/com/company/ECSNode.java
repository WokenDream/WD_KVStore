package com.company;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

/**
 * Created by tianqiliu on 2018-02-24.
 */
public class ECSNode implements IECSNode, Serializable {

    private static String HASH_ALGO = "MD5";

    public enum Action {
        Start, Stop, Kill
    }

    private String name;
    private String ipAddress;
    private int port;
    private String[] hashRange; // (predecessor, me]
    private TreeMap<String, String> hashRing = new TreeMap<>(); // (hash, nodeName: may need to change)

    private int cacheSize;
    private String cacheStrategy;

    // stated vars
    public boolean inUse = false;
    public boolean connected = false;
    public boolean started = false;
    public boolean stopped = true;
    public boolean killed = false;
    public Action todo;

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

    public void setNodepredecessor(String predecessorHash) {
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
}
