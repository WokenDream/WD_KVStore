package com.company;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by tianqiliu on 2018-02-24.
 */
public class ECSNode implements IECSNode, Serializable {

    private static String HASH_ALGO = "MD5";

    private String name;
    private String ipAddress;
    private int port;
    private String[] hashRange; // (predecessor, me]

//    private Process process;
//    private boolean instantiated = false;
    private int cacheSize;
    private String cacheStrategy;

    public ECSNode(String name, String ipAddress, int port, int cacheSize, String cacheStrategy) {
        initNode(name, ipAddress, port, cacheSize, cacheStrategy);
    }

    public ECSNode(String ipAddress, int port, int cacheSize, String cacheStrategy) {
        initNode(ipAddress + ":" + port, ipAddress, port, cacheSize, cacheStrategy);
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
    private void initNode(String name, String ipAddress, int port, int cacheSize, String cacheStrategy) {
        this.name = name;
        this.ipAddress = ipAddress;
        this.port = port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        hashRange = new String[2];
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGO);
            byte[] bytes = messageDigest.digest((ipAddress + ":" + port).getBytes());
            hashRange[1] = DatatypeConverter.printHexBinary(bytes);
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

//    public void startServer(int cacheSize, String cacheStrategy) throws IOException {
//        if (instantiated) {
//            throw new IOException("Server is already instantiated");
//        }
//        if (cacheSize < 1) {
//            throw new IllegalArgumentException("Cache size " + cacheSize + " is invalid");
//        }
//        if (cacheStrategy == null || cacheStrategy.equals("")) {
//            throw new IllegalArgumentException("invalid cache eviction policy");
//        }
//
//        cacheStrategy = cacheStrategy.toUpperCase();
//        if (!(cacheStrategy.equals("FIFO") || cacheStrategy.equals("LRU") || cacheStrategy.equals("LFU") || cacheStrategy.equals("None"))) {
//            throw new IllegalArgumentException("invalid cache eviction policy " + cacheStrategy);
//        }
//
//        this.cacheStrategy = cacheStrategy;
//        //TODO: complete path and argument of server
//        String cmd = "ssh -n " + ipAddress + " nohup java -jar <path>/ms2-server.jar " + port + "blabla";
//        process = Runtime.getRuntime().exec(cmd);
//        instantiated = true;
//    }

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
}
