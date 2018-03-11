package com.company;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Set;

/**
 * Created by tianqiliu on 2018-03-11.
 */
public class ECSCli {
    private static boolean running = true;
    private static final String PROMPT = "ECSClient> ";
    private static ECSClient client;

    public static void main(String[] args) {
        String configPath = ECSClient.configPath;
        if (args == null || args.length == 0) {
            System.out.println("No config file provided. Using the default config file");
        } else if (args.length == 1) {
            configPath = args[0];
        } else {
            System.out.println("You can optionally provide just one argument as the config file path");
            System.out.println("exiting");
            System.exit(0);
        }
        String cmdLine;
        System.out.println("type help to view the list of available commands");

        try {
            client = new ECSClient("localhost", 2181);
        } catch (IOException e) {
            System.out.println("failed to launch ECS client");
            System.out.println(e.getLocalizedMessage());
            running = false;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (running) {
            System.out.print(PROMPT);
            try {
                cmdLine = reader.readLine();
                handleCmd(cmdLine);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
            }


        }
    }

    private static void printSuccess() {
        System.out.println("success");
    }

    private static void printFail() {
        System.out.println("fail");
    }

    private static void handleCmd(String cmdLine) throws Exception {
        // TODO: remember to prepend '/' to nodeName when using zookeeper
        String[] tokens = cmdLine.toLowerCase().split(" ");
        String cmd = tokens[0];
        switch (cmd) {
            case  "start":
                if (client.start()) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "stop":
                if (client.stop()) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "shutdown":
                if (client.shutdown()) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "addnode":
                if (handleAddNode(tokens)) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "addnodes":
                if (handleAddNodes(tokens)) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "removenode":
                if (handleRemoveNode(tokens)) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "removenodes":
                if (handleRemoveNodes(tokens)) {
                    printSuccess();
                } else {
                    printFail();
                }
                break;
            case "get":
                handleGet(tokens);
            case "exit":
                client.shutdown();
                running = false;
                break;
            default:
                printHelp();
                break;
        }
    }

    private static boolean handleAddNode(String[] tokens) throws Exception {
        if (tokens.length != 3) {
            throw new Exception("incorrect arguments are addnode");
        }
        String strategy = tokens[1];
        if (strategy.equals("fifo") || strategy.equals("lru") || strategy.equals("lfu")) {
            throw new Exception("cache strategy can only be FIFO, LRU or LFU");
        }
        int size = 5;
        try {
            size = Integer.parseInt(tokens[2]);
        } catch (NumberFormatException e) {
            throw new Exception("cache size is in valid");
        }
        if (size < 1) {
            throw new Exception("cache size is in valid");
        }

        return client.addNode(strategy, size) != null;
    }

    private static boolean handleAddNodes(String[] tokens) throws Exception {
        if (tokens.length != 4) {
            throw new Exception("incorrect arguments are addnodes");
        }

        int count = 1;
        try {
            count = Integer.parseInt(tokens[1]);
        } catch (NumberFormatException e) {
            throw new Exception("count is in valid");
        }
        if (count < 1) {
            throw new Exception("count is in valid");
        }

        String strategy = tokens[2];
        if (strategy.equals("fifo") || strategy.equals("lru") || strategy.equals("lfu")) {
            throw new Exception("cache strategy can only be FIFO, LRU or LFU");
        }

        int size = 5;
        try {
            size = Integer.parseInt(tokens[2]);
        } catch (NumberFormatException e) {
            throw new Exception("cache size is in valid");
        }
        if (size < 1) {
            throw new Exception("cache size is in valid");
        }

        return client.addNodes(count, strategy, size) != null;
    }

    private static boolean handleRemoveNode(String[] tokens) throws Exception {
        if (tokens.length != 2) {
            throw new Exception("invalid arguments for removenode");
        }

        String name = tokens[1].charAt(0) == '/' ? tokens[0] : "/" + tokens[0];
        return client.removeNode(name);
    }

    private static boolean handleRemoveNodes(String[] tokens) throws Exception {
        if (tokens.length < 2) {
            throw new Exception("invalid arguments for removenodes");
        }

        Collection<String> names = new ArrayDeque<>();
        for (int i = 1; i < tokens.length; ++i) {
            names.add(tokens[i]);
        }
        return client.removeNodes(names);
    }

    private static void handleGet(String[] tokens) throws Exception {
        if (tokens.length != 2) {
            throw new Exception("invalid arguments for get, either znodes or nodes");
        }
        switch (tokens[1]) {
            case "znodes":
                handleGetZnodes();
                break;
            case "nodes":
                handleGetNodes();
                break;
            default:
                throw new Exception("invalid arguments for get, either znodes or nodes");
        }
    }

    private static void handleGetZnodes() {
        Set<String> names = client.getZnodeNames();
        if (names == null || names.size() == 0) {
            System.out.println("no znodes");
            return;
        }
        for (String name: names) {
            System.out.println(name);
        }
    }

    private static void handleGetNodes() {
        Set<String> names = client.getZnodeNames();
        if (names == null || names.size() == 0) {
            System.out.println("no server available");
            return;
        }
        for (String name: names) {
            System.out.println(name);
        }
    }

    private static void printHelp() {
        StringBuilder sb = new StringBuilder();
        final String prefix = "\t";
        final String infix = " --- ";
        sb.append("List of commands (case-insensitive):\n");

        sb.append(prefix);
        sb.append("addnode cacheStrategy cacheSize");
        sb.append(infix);
        sb.append("add a server with the specified cache strategy and size.\n");

        sb.append(prefix);
        sb.append("addnodes count cacheStrategy cacheSize");
        sb.append(infix);
        sb.append("add a number of server with the specified cache strategy and size.\n");

        sb.append(prefix);
        sb.append("start");
        sb.append(infix);
        sb.append("start all the participating servers.\n");

        sb.append(prefix);
        sb.append("stop");
        sb.append(infix);
        sb.append("halt all the participating servers.\n");

        sb.append(prefix);
        sb.append("removenode nodeName");
        sb.append(infix);
        sb.append("remove a participating server named nodeName.\n");

        sb.append(prefix);
        sb.append("removenodes nodeName1 nodeName2 ... ");
        sb.append(infix);
        sb.append("remove a number of participating servers named nodeName1 nodeName2 and etc.\n");

        sb.append(prefix);
        sb.append("shutdown");
        sb.append(infix);
        sb.append("kill all the participating servers.\n");

        sb.append(prefix);
        sb.append("exit");
        sb.append(infix);
        sb.append("kill all the participating servers and exit ECS client\n");

        sb.append(prefix);
        sb.append("get znodes");
        sb.append(infix);
        sb.append("print all the znodes\n");

        sb.append(prefix);
        sb.append("get nodes");
        sb.append(infix);
        sb.append("print all the servers listed in config file\n");

        sb.append(prefix);
        sb.append("help");
        sb.append(infix);
        sb.append("list all the commands their usage\n");

        System.out.println(sb.toString());
    }
}
