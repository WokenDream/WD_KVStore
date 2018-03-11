package com.company;
import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * Created by tianqiliu on 2018-03-03.
 */

/**
 * This is the only class allowed to directly modify the data member of ECSNode
 */
class ECSNodeManager {
    private HashMap<String, ECSNode> allNodes = new HashMap<>(); // (nodeName, node)
    private int numOfFreeNodes = 0;

    public ECSNodeManager(String configPath) throws IOException {
        numOfFreeNodes = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(configPath));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(" ");
                String nodeName = tokens[0].charAt(0) == '/' ? tokens[0] : "/" + tokens[0];
                String ip = tokens[1];
                int port = Integer.parseInt(tokens[2]);
                ECSNode node = new ECSNode(nodeName, ip, port);
                allNodes.put(nodeName, node);
                ++numOfFreeNodes;
            }
        } catch (FileNotFoundException e) {
            System.out.println("config file not found: " + configPath);
            System.out.println(e.getLocalizedMessage());
        } catch (IOException e) {
            System.out.println("config file corrupted");
            System.out.println(e.getLocalizedMessage());
        }
    }

    /**
     * This is the only method on client side that allows to change inUse state of node
     * @param node
     * @param inUse
     */
    public void setNodeInUse(ECSNode node, boolean inUse) {
        if (node.inUse != inUse) {
            node.inUse = inUse;
            numOfFreeNodes = inUse ? numOfFreeNodes - 1 : numOfFreeNodes + 1;
        } else {
            System.out.println("dude, this node is already in use: " + node.getNodeName());
        }

    }

    public int getTotalNumberOfNodes() {
        return allNodes.size();
    }

    public IECSNode getNode(String nodeName) {
        return allNodes.get(nodeName);
    }

    public Collection<ECSNode> getNodes() {
        return allNodes.values();
    }

    public Set<Map.Entry<String ,ECSNode>> getEntrySet() {
        return allNodes.entrySet();
    }

    public int getNumOfAvailableNodes() {
        return numOfFreeNodes;
    }

    public Map<String, IECSNode> getNodeMap() {
        HashMap<String, IECSNode> nodes = new HashMap<>();
        for (Map.Entry<String, ECSNode> entry: allNodes.entrySet()) {
            nodes.put(entry.getKey(), entry.getValue());
        }
        return nodes;
    }

}

public class ECSClient implements IECSClient {
    private ZooKeeper zk;
    private CountDownLatch countDownLatch = new CountDownLatch(1);// may be unnecessary
    private HashMap<String, ECSNode> znodeHashMap = new HashMap<>(); // (znodePath i.e. nodeName, ecsnode)
    private TreeMap<String, IECSNode> hashRing = new TreeMap<>(); // (hash, znodepath)
    private HashMap<String, Process> processHashMap = new HashMap<>(); // (znodePath i.e. nodeName, processes)
    private static String configPath = "ecs.config";
    private String zkIpAddress = "localhost";
    private int zkPort = 2181;
    private int sessionTimeout = 3000;

    private ECSNodeManager allNodes;

    public ECSClient(String configPath, String zkIpAddress, int zkPort, int sessionTimeout) throws IOException {
        allNodes = new ECSNodeManager(configPath);
        this.configPath = configPath;
        this.zkIpAddress = zkIpAddress;
        this.zkPort = zkPort;
        this.sessionTimeout = sessionTimeout;
        connectToZookeeper();
    }

    public ECSClient(String zkIpAddress, int zkPort) throws IOException {
        allNodes = new ECSNodeManager(configPath);
        this.zkIpAddress = zkIpAddress;
        this.zkPort = zkPort;
        connectToZookeeper();

    }

    //------------------custom implementation---------------//

    /**
     * Connect to zookeeper; block until connection is made
     * @throws IOException
     */
    private void connectToZookeeper() throws IOException {
        zk = new ZooKeeper(zkIpAddress + ":" + zkPort, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO: logging
            System.out.println(e.getLocalizedMessage());
            System.out.println("failed to  initialize ECS Client, exiting");
            System.exit(-1);
        }
    }

    /**
     * Run a KVServer process
     * @param node ECS node which contains metadata for the KVServer
     * @return success if server is started , false otherwise
     */
    private boolean startNodeServer(IECSNode node) {
        // TODO: complete path and argument of server
        try {
            String cmd = "ssh -n " + node.getNodeHost() + " nohup java -jar <path>/ms2-server.jar " + node.getNodePort() + "blabla";
            Process process = Runtime.getRuntime().exec(cmd);
            processHashMap.put(node.getNodeName(), process);
        } catch (IOException e) {
            System.out.println("failed to launch server: " + node.getNodeName());
            System.out.println(e.getLocalizedMessage());
            return false;

        }
        return true;
    }

    /**
     * Run a collection of KVServer processes
     * @param nodes
     * @return success if all servers are started , false otherwise
     */
    private boolean startNodeServers(Collection<IECSNode> nodes) {
        for (IECSNode node: nodes) {
            if (!startNodeServer(node)) {
                return false;
            }
        }
        return true;
    }

    /**
     * find an available node and mark the found node as not available (false)
     * @param cacheStrategy
     * @param cacheSize
     * @return
     */
    private ECSNode popAvailableNode(String cacheStrategy, int cacheSize) {
        for (ECSNode node: allNodes.getNodes()) {
            if (!node.inUse) {
                node.setCache(cacheStrategy, cacheSize);
                allNodes.setNodeInUse(node, true);
                return node;
            }
        }
        return null;
    }

    /**
     * Create a znode
     * @param cacheStrategy
     * @param cacheSize
     * @return znode path to the znode created
     */
    private ECSNode setupNode(String cacheStrategy, int cacheSize) {
        ECSNode node = popAvailableNode(cacheStrategy, cacheSize);
        if (node == null || !createZnode(node)) {
            return null;
        }
        if (!updateHashRingOfEveryZnodeUsing(node, true)) {
            removeNode(node.getNodeName(), true);
            return null;
        }
        return node;
    }

    /**
     * wait for a node to connect/disconnect
     * @param znodePath
     * @param timeoutMilli
     * @param connected whether expect connect/disconnect state from server
     * @return whether or not the server successfully connects/disconnects
     */
    private boolean awaitNode(String znodePath, int timeoutMilli, boolean connected) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeoutMilli);
        } catch (InterruptedException e) {
            System.out.println("ECS Client exiting due to interrupted exception");
            System.exit(-1);
        }
        ECSNode newNode = checkNodeConnected(znodePath, connected);
        if (newNode == null) {
            return false;
        }
        znodeHashMap.put(znodePath, newNode);
        return true;
    }

    /**
     * check if znode if is connected/disconnected
     * @param znodePath path to znode
     * @param connected status wants to check
     * @return the ecs node converted from the znode updated by KVServer if successful
     */
    private ECSNode checkNodeConnected(String znodePath, boolean connected) {
        try {
            Stat stat = zk.exists(znodePath, true);
            byte[] bytes = zk.getData(znodePath, true, stat);
            ECSNode node = ECSNode.fromBytes(bytes);
            if (node.connected == connected) {
                return node;
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            if (e instanceof IOException) {
                System.out.println("failed to deserialize node " + znodePath);
            } else if (e instanceof InterruptedException) {
                System.out.println("ECS Client exiting due to interrupted exception");
                System.exit(-1);
            }
        }
        return null;
    }

    /**
     * remove a node completely: remove from client memory, kill the server and delete znode
     * @param nodeName
     * @return
     */
    private boolean removeNode(String nodeName, boolean updateHashRing) {
        // remove memory representation of node
        ECSNode node = znodeHashMap.remove(nodeName);
        if (node == null || !node.inUse) {
            return false;
        }
        allNodes.setNodeInUse(node, false);

        node.todo = ECSNode.Action.Kill;

        if (processHashMap.containsKey(nodeName)) {
            try {
                // kill server and remove znode
                Stat stat = zk.exists(nodeName, true);
                stat = zk.setData(nodeName, node.toBytes(), stat.getVersion());
                // TODO: wait till server exits?
                zk.delete(nodeName, stat.getVersion());
                if (updateHashRing) {
                    updateHashRingOfEveryZnodeUsing(node, false);
                }
                processHashMap.remove(nodeName);
            } catch (KeeperException e) {
                System.out.println(e.getLocalizedMessage());
            } catch (InterruptedException e) {
                System.out.println("ECS client existing due to interrupted exception");
                System.exit(-1);
            } catch (IOException e) {
                System.out.println("failed to serialize node " + nodeName);
            }
        } else {
            return false;
        }


        return true;
    }

    /**
     * Create a znode using the given ecs node
     * @param node
     * @return true if successful
     */
    private boolean createZnode(ECSNode node) {
        try {
            String znodePath = zk.create(node.getNodeName(), node.toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if (!znodePath.equals(node.getNodeName())) { // this shouldn't happen
                node.setNodeName(znodePath);
                Stat stat = zk.exists(znodePath, true);
                zk.setData(znodePath, node.toBytes(), stat.getVersion());
            }
            znodeHashMap.put(znodePath, node);
        } catch (Exception e) {
            if (e instanceof KeeperException.InvalidACLException) {
                System.out.println("the ACL is invalid, null, or empty");
            } else if (e instanceof KeeperException) {
                System.out.println("the zookeeper server returns a non-zero error code");
            } else if (e instanceof InterruptedException) {
                System.out.println("exiting ECS Client due to interrupted exception");
                System.out.println(e.getLocalizedMessage());
                // TODO: may be need to do some clean up e.g. zookeeper
                System.exit(-1);
            }

            // TODO: may need more cleanup
            allNodes.setNodeInUse(node, false);
            System.out.println(e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    /**
     * Update the hash ring and predecessor of a znode and ecsnode
     * @param newNode newly created ecsnode
     * @return
     */
    private boolean updateHashRingOfEveryZnodeUsing(ECSNode newNode, boolean toAdd) {
        // update hash ring of every one
        if (toAdd) {
            hashRing.put(newNode.getNodeHash(), newNode);
        } else if (hashRing.remove(newNode) == null) {
            return false;
        }

        boolean success = true;
        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode tempNode = entry.getValue();
            if (!updateHashRingOfEveryZnodeHelper(znodePath, tempNode)) {
                success =  false;
            }
        }
        return success;
    }

    /**
     * Update the hash ring and predecessors of a collection znodes and ecsnodes
     * @param nodes
     * @return
     */
    private boolean updateHashRingOfEveryZnodeUsing(Collection<IECSNode> nodes, boolean toAdd) {
        boolean success = true;
        if (toAdd) {
            for (IECSNode node: nodes) {
                hashRing.put(node.getNodeHashRange()[1], node);
            }
        } else {
            for (IECSNode node: nodes) {
                if (hashRing.remove(node.getNodeHashRange()[1], node)) {
                    success = false;
                }
            }
        }

        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode tempNode = entry.getValue();
            if (!updateHashRingOfEveryZnodeHelper(znodePath, tempNode) ) {
                success = false;
            }
        }
        return success;
    }

    /**
     * Helper function for setting the hash ring and predecessor of node
     * The updates to the global hash ring should be done before calling this function
     * @param znodePath
     * @param node
     * @return
     */
    private boolean updateHashRingOfEveryZnodeHelper(String znodePath, ECSNode node) {
        node.hashRing = hashRing;
        // update predecessor
        String predecessor = hashRing.lowerKey(node.getNodeHash());
        if (predecessor == null) {
            predecessor = hashRing.lastKey();
        }
        node.setNodeHashLowRange(predecessor);
        try {
            Stat stat = zk.exists(znodePath, true);
            zk.setData(znodePath, node.toBytes(), stat.getVersion());
        } catch (InterruptedException e) {
            System.out.println("ECS Client existing due to interrupted exception");
            System.exit(-1);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            if (e instanceof IOException) {
                System.out.println("failed to serialie node: " + node.getNodeName());
            }
            return false;
        }
        return true;
    }

    //---------------IECSClient Implemntation---------------//
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception {
        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode node = entry.getValue();
            Stat stat = zk.exists(znodePath, true);
            if (stat == null) {
                throw new Exception("node " + node.getNodeName() + " does not exist at path: " + znodePath);
            }
            node.todo = ECSNode.Action.Start;
            zk.setData(znodePath, node.toBytes(), stat.getVersion());
        }
        // TODO: may need to await
        return true;
    }

    /**
     * Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean stop() throws Exception {
        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode node = entry.getValue();
            Stat stat = zk.exists(znodePath, true);
            if (stat == null) {
                throw new Exception("node " + node.getNodeName() + " does not exist at path: " + znodePath);
            }
            node.todo = ECSNode.Action.Stop;
            zk.setData(znodePath, node.toBytes(), stat.getVersion());
        }

        // TODO: may need to await
        return false;
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception {
        if (!stop()) {
            return false;
        }
        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode node = entry.getValue();
            Stat stat = zk.exists(znodePath, true);
            if (stat == null) {
                throw new Exception("node " + node.getNodeName() + " does not exist at path: " + znodePath);
            }
            node.todo = ECSNode.Action.Kill;
            zk.setData(znodePath, node.toBytes(), stat.getVersion());
        }
        // TODO: may need to await
//        for (Process proc: processHashMap.values()) {
//            if (proc.isAlive()) {
////                proc.destroy();
//                proc.destroyForcibly();
//            }
//
//        }
        for (ECSNode node: znodeHashMap.values()) {
            allNodes.setNodeInUse(node, false);
        }
        // TODO: should remove znodes?
        znodeHashMap.clear();

        return false;
    }

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     */
    // TODO: logging in exceptions
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        ECSNode node = setupNode(cacheStrategy, cacheSize);
        if (node == null) {
            return null;
        }
        if (!startNodeServer(node) || awaitNode(node.getNodeName(), sessionTimeout, true)) {
            removeNode(node.getNodeName(), true);
        }
        return node;
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     * @return  set of strings containing the names of the nodes
     */
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if (nodes != null) {
            return null;
        }
        if (!startNodeServers(nodes)) {
            // remove nodes
            for (IECSNode node: nodes) {
                removeNode(node.getNodeName(), false);
            }
            updateHashRingOfEveryZnodeUsing(nodes, false);
            return null;
        }
        try {
            if (!awaitNodes(count, sessionTimeout)) {
                for (IECSNode node: nodes) {
                    removeNode(node.getNodeName(), false);
                }
                updateHashRingOfEveryZnodeUsing(nodes, false);
            }
        } catch (Exception e) {
            // TODO: check when exception is thrown
            System.out.println(e.getLocalizedMessage());
        }
        return nodes;
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * @return  array of strings, containing unique names of servers
     */
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (allNodes.getNumOfAvailableNodes() < count) {
            System.out.println("not enough free nodes available");
            return null;
        }
        ArrayList<IECSNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            ECSNode node = popAvailableNode(cacheStrategy, cacheSize);
            if (node == null || !createZnode(node)) {
                return null;
            }
            nodes.add(node);
        }
        if (!updateHashRingOfEveryZnodeUsing(nodes, true)) {
            // remove nodes
            for (IECSNode node: nodes) {
                removeNode(node.getNodeName(), false);
            }
            updateHashRingOfEveryZnodeUsing(nodes, false);
        }
        return nodes;

    }

    /**
     * Wait for all nodes to report status or until timeout expires
     * @param count     number of nodes to wait for
     * @param timeout   the timeout in milliseconds
     * @return  true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) throws Exception {
        if (count > allNodes.getTotalNumberOfNodes()) {
            System.out.println("total number of nodes: " + allNodes.getTotalNumberOfNodes());
            System.out.println("requested number of nodes" + count);
        }
        TimeUnit.MILLISECONDS.sleep(timeout);
        int i = 0;
        // find nodes (up to count) that were not connected, and check if they are now connected
        for (Map.Entry<String, ECSNode> entry: znodeHashMap.entrySet()) {
            String znodePath = entry.getKey();
            ECSNode node = entry.getValue();
            if (!node.connected) {
                ECSNode newNode = checkNodeConnected(znodePath, true);
                if (newNode != null) {
                    // update
                    znodeHashMap.put(znodePath, newNode);
                    ++i;
                } else {
                    System.out.println("node " + znodePath + " failed to connect");
                    return false;
                }

            }
            if (i >= count) {
                break;
            }
        }
        return true;
    }

    /**
     * Removes nodes with names matching the nodeNames array
     * @param nodeNames names of nodes to remove
     * @return  true on success, false otherwise
     */
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO: see removeNode
        boolean allRemoved = true;
        ArrayList<IECSNode> nodes = new ArrayList<>();
        ECSNode node;
        for (String nodeName: nodeNames) {
            if (!removeNode(nodeName, false)) {
                allRemoved = false;
            }
            node = znodeHashMap.get(nodeName);
            if (node != null) {
                nodes.add(node);
            } else {
                System.out.println("node: " + nodeName + " is not found in znodeHashMap");
            }
        }
        updateHashRingOfEveryZnodeUsing(nodes, false);
        return allRemoved;
    }

    /**
     * Get a map of all nodes
     */
    public Map<String, IECSNode> getNodes() {
        return allNodes.getNodeMap();
    }

    /**
     * Get the specific node responsible for the given key
     */
    public IECSNode getNodeByKey(String Key) {
        return allNodes.getNode(Key);
    }

    //--------------end of IECSClient implementation------------//

    private static boolean running = true;
    private static final String PROMPT = "ECSClient> ";
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

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (running) {
            System.out.print(PROMPT);
            try {
                cmdLine = reader.readLine();
                handleCmd(cmdLine);
            } catch (IOException e) {
                running = false;
            }


        }
    }

    private static void handleCmd(String cmdLine) {
        String[] tokens = cmdLine.toLowerCase().split(" ");
        String cmd = tokens[0];
        switch (cmd) {
            case  "start":
                break;
            case "stop":
                break;
            case "shutdown":
                break;
            case "addnode":
                break;
            case "addnodes":
                break;
            case "removenode":
                break;
            case "removenodes":
                break;
            case "exit":
                // TODO: kill the servers
                running = false;
                break;
            default:
                printHelp();
                break;
        }
    }

    private static void printHelp() {
        StringBuilder sb = new StringBuilder();
        String prefix = "\t";
        sb.append(prefix);
        sb.append("List of commands (case-insensitive):\n");
        sb.append(prefix);
        sb.append("'addnode cacheStrategy cacheSize': add a server with the specified cache strategy and size.\n");
        sb.append(prefix);
        sb.append("'addnodes count cacheStrategy cacheSize': add a number of server with the specified cache strategy and size.\n");
        sb.append(prefix);
        sb.append("'start': start all the participating servers.\n");
        sb.append(prefix);
        sb.append("'stop': halt all the participating servers.\n");
        sb.append(prefix);
        sb.append("'removenode': remove a participating server.\n");
        sb.append(prefix);
        sb.append("'removenodes count': remove a number of participating servers.\n");
        sb.append(prefix);
        sb.append("'shutdown': kill all the participating servers.\n");
        sb.append(prefix);
        sb.append("'exit': kill all the participating servers and exit ECS client\n");
        sb.append(prefix);
        sb.append("'help': list all the commands their usage\n");
        System.out.println(sb.toString());
    }
}
