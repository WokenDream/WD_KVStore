package com.company;

public class Main {

    public static void main(String[] args) {
	// write your code here
        KVCache cache = new KVCache(500, CacheStrategy.LRU);
        for (int i = 0; i < 100; ++i) {
            cache.putKV("" + i, i + " abcd- e78hsds");
        }
        for (int i = 0; i < 100; ++i) {
            String val = cache.getKV("" + i);
            System.out.println(val);
        }
    }
}
