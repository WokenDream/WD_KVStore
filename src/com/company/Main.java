package com.company;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Main {

    public static void main(String[] args) {
	// write your code here

        try {
            KVSimpleStorage db = new KVStorage("./newDB/", 400, IKVServer.CacheStrategy.LRU);
            db.putKV("k1", "k2");
            System.out.println(db.inStorage("k1"));
            System.out.println(db.inCache("k1"));
            System.out.println(db.getKV("k1").getValue());
            db.clearStorage();
            System.out.println(db.inStorage("k1"));
            System.out.println(db.inCache("k1"));
            System.out.println(db.getKV("k1").getValue());
//            String key = "updateTestValue";
//            String val = "initial";
//            KVStorageResult result = db.putKV(key, val);
//            System.out.println(result.getResult());
//            result = db.getKV(key);
//            System.out.println(result.getValue());
//            val = "updated";
//            result = db.putKV(key, val);
//            System.out.println(result.getResult());
//            result = db.getKV(key);
//            System.out.println(result.getValue());
//            result = db.putKV(key, "null");
//            System.out.println(result.getResult());

//            for (int i = 0; i < 100; ++i) {
//                db.putKV("" + i, i + " abcd- e78hsds");
//            }
//
//            for (int i = 0; i < 100; ++i) {
//                String val = db.getKV("" + i).getValue();
//                System.out.println(val);
//            }
//
//            for (int i = 0; i < 100; ++i) {
//                db.putKV("" + i, (i << 1) + " abcd- e78hsds");
//            }
//
//            for (int i = 0; i < 100; ++i) {
//                String val = db.getKV("" + i).getValue();
//                System.out.println(val);
//            }
//
//            db.clearStorage();
//            KVStorageResult result = db.getKV("0");
//            System.out.println(result.getValue());

//            ECSClient ecsClient = new ECSClient("localhost", 2181, 3000);
            TreeMap<String, String> treeMap = new TreeMap<>();
            treeMap.put("5", "3");
            treeMap.put("3", "4");
            treeMap.put("4", "4");
            for (String key: treeMap.keySet()) {
                System.out.println(key);
            }
            for (Map.Entry<String, String> entry :treeMap.entrySet()) {
                System.out.println(entry.getKey());
            }
//            ECSClient client = new ECSClient("./ecs.config", "localhost", 2181, 3000);
//            client.addNode("LRU", 3000);
//            client.setupNode("FIFO", 3333);
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }

    }
}
