package com.company;
import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

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
                String nodeName = tokens[0];
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

    public void setNodeInUse(ECSNode node, boolean inUse) {
        if (node.inUse != inUse) {
            node.inUse = inUse;
            numOfFreeNodes = inUse ? numOfFreeNodes - 1 : numOfFreeNodes + 1;
        } else {
            System.out.println("dude, this node is already in use: " + node.getNodeName());
        }

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
    private HashMap<String, ECSNode> znodeHashMap = new HashMap<>(); // (znodePath, ecsnode)
    private HashMap<String, Process> processHashMap = new HashMap<>(); // (znodePath, processes)
    private String zkIpAddress = "localhost";
    private int zkPort = 3000;
    private int sessionTimeout = 3000;

    private ECSNodeManager allNodes;

    public ECSClient(String zkIpAddress, int zkPort, int sessionTimeout) throws IOException {
        allNodes = new ECSNodeManager("somepath");
        this.zkIpAddress = zkIpAddress;
        this.zkPort = zkPort;
        this.sessionTimeout = sessionTimeout;
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
     * @param znodePath path given by zookeeper
     * @param node ECS node which contains metadata for the KVServer
     * @throws IOException
     */
    private void startNodeServer(String znodePath, IECSNode node) throws IOException {
        // TODO: complete path and argument of server
        String cmd = "ssh -n " + node.getNodeHost() + " nohup java -jar <path>/ms2-server.jar " + node.getNodePort() + "blabla";
        Process process = Runtime.getRuntime().exec(cmd);
        processHashMap.put(znodePath, process);
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
    private String setupNode(String cacheStrategy, int cacheSize) {
        ECSNode node = popAvailableNode(cacheStrategy, cacheSize);
        String znodePath = null;
        if (node == null) {
            return null;
        }
        try {
            byte[] bytes = node.toBytes();
            znodePath = zk.create(node.getNodeHash(), bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            znodeHashMap.put(znodePath, node);
        } catch (Exception e) {
            if (e instanceof KeeperException.InvalidACLException) {
                System.out.println("the ACL is invalid, null, or empty");
            } else if (e instanceof KeeperException) {
                System.out.println("the zookeeper server returns a non-zero error code");
            } else if (e instanceof InterruptedException) {
                System.out.println("exiting client due to interrupted exception");
                System.out.println(e.getLocalizedMessage());
                // TODO: may be need to do some clean up e.g. zookeeper
                System.exit(-1);
            }

            // TODO: may need more cleanup
            allNodes.setNodeInUse(node, false);
            znodeHashMap.remove(znodePath);
            System.out.println(e.getLocalizedMessage());
            return null;
        }

        return znodePath;

    }

    private boolean awaitNode(int timeout) {
        // TODO: not sure what to do; may call get after create znode and set a watcher
        // TODO: KVServer change znode state when connected
        return false;
    }

    private boolean removeNode(String nodeName) {
        // TODO: does it kill the server?
        return false;
    }

    //---------------IECSClient Implemntation---------------//
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception {
        // TODO: initialize hash ring
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
        return false;
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception {
        // TODO: should shutdown remove znode?
        stop();
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
        znodeHashMap.clear();

        return false;
    }

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     */
    // TODO: logging in exceptions
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String znodePath = setupNode(cacheStrategy, cacheSize);
        if (znodePath == null) {
            return null;
        }
        ECSNode node = znodeHashMap.get(znodePath);
        try {
            startNodeServer(znodePath, node);
        } catch (IOException e) {
            System.out.println("failed to launch node: " + node.getNodeName());
            System.out.println(e.getLocalizedMessage());
            znodeHashMap.remove(znodePath);
            allNodes.setNodeInUse(node, false);
        }

        if (!awaitNode(sessionTimeout)) {
            // clean up: delete node and destroy process
            return null;
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
//        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
//        if (nodes == null) {
//            return null;
//        }
//        for (IECSNode node: nodes) {
//
//        }
        if (allNodes.getNumOfAvailableNodes() < count) {
            System.out.println("not enough free nodes available");
            return null;
        }
        ArrayList<IECSNode> nodes = new ArrayList<>();
        IECSNode node;
        for (int i = 0; i < count; ++i) {
            node = addNode(cacheStrategy, cacheSize);
            if (node == null) {
                // TODO: not sure if reverse what have been done
                return null;
            }
            nodes.add(node);
        }

        // call awaitNodes
        try {
            awaitNodes(count, 3000);
        } catch (Exception e) {
            System.out.println("Server connection timed out");
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
            String znothPath = setupNode(cacheStrategy, cacheSize);
            nodes.add(znodeHashMap.get(znothPath));
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
        // TODO: not sure what to do
        for (int i = 0; i < count; ++i) {
            if (!awaitNode(timeout)) {
                return false;
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
        for (String nodeName: nodeNames) {
            if (!removeNode(nodeName)) {
                allRemoved = false;
            }
        }
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
}
