package com.company;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by tianqiliu on 2018-02-24.
 */
public class ECSNode implements IECSNode {

    private static String HASH_ALGO = "MD5";

    private String name;
    private String host;
    private int port;
    private String hash;
    private String[] hashRange; // (predecessor, me]

    private Process process;
    private boolean instantiated = false;
    private int cacheSize;
    private String cacheStrategy;

    public ECSNode(String name, String host, int port, int cacheSize, String cacheStrategy) throws IOException {
        init(name, host, port);
        startServer(cacheSize, cacheStrategy);
    }

    public ECSNode(String name, String host, int port) {
        init(name, host, port);
    }

    public ECSNode(String host, int port) {
        init("Server " + host, host, port);
    }

    //----------------IECSNode implementation----------------//
    public String getNodeName() {
        return name;
    }

    public String getNodeHost() {
        return host;
    }

    public int getNodePort() {
        return port;
    }

    public String[] getNodeHashRange() {
        return hashRange;
    }

    //------------------Custom Methods------------------//
    private void init(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGO);
            byte[] bytes = messageDigest.digest((host + ":" + port).getBytes());
            hash = DatatypeConverter.printHexBinary(bytes);
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getLocalizedMessage());
        }
        hashRange = new String[2];
        hashRange[1] = hash;
    }

    public void startServer(int cacheSize, String cacheStrategy) throws IOException {
        if (instantiated) {
            throw new IOException("Server is already instantiated");
        }
        if (cacheSize < 1) {
            throw new IllegalArgumentException("Cache size " + cacheSize + " is invalid");
        }
        if (cacheStrategy == null || cacheStrategy.equals("")) {
            throw new IllegalArgumentException("invalid cache eviction policy");
        }

        cacheStrategy = cacheStrategy.toUpperCase();
        if (!(cacheStrategy.equals("FIFO") || cacheStrategy.equals("LRU") || cacheStrategy.equals("LFU") || cacheStrategy.equals("None"))) {
            throw new IllegalArgumentException("invalid cache eviction policy " + cacheStrategy);
        }

        this.cacheStrategy = cacheStrategy;
        //TODO: complete path and argument of server
        String cmd = "ssh -n " + host + " nohup java -jar <path>/ms2-server.jar " + port + "blabla";
        process = Runtime.getRuntime().exec(cmd);
        instantiated = true;
    }

    public String getNodeHash() {
        return hash;
    }

    public void setNodeHashRange(String predecessorHash) {
        hashRange[0] = predecessorHash;
    }

    public int getNodeCacheSize() {
        return cacheSize;
    }

    public String getNodeCacheStrategy() {
        return cacheStrategy == null ? "" : cacheStrategy;
    }
}
