package com.company;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
	// write your code here

        try {
            KVSimpleStorage db = new KVStorage("./newDB/", 400, IKVServer.CacheStrategy.LRU);
            String key = "updateTestValue";
            String val = "initial";
            KVStorageResult result = db.putKV(key, val);
            System.out.println(result.getResult());
            result = db.getKV(key);
            System.out.println(result.getValue());
            val = "updated";
            result = db.putKV(key, val);
            System.out.println(result.getResult());
            result = db.getKV(key);
            System.out.println(result.getValue());
            result = db.putKV(key, "null");
            System.out.println(result.getResult());

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

        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }

    }
}
