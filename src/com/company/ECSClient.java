package com.company;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;

/**
 * Created by tianqiliu on 2018-03-03.
 */

class ECSNodes {
    private HashMap<String, Boolean> allNodes = new HashMap<>(); // (node's ip + port, launched or not)
    private int count = 0;

    public void setInUse(String address, Boolean used) {
        allNodes.put(address, used);
        count = used ? count - 1 : count + 1;
    }

    public Set<Map.Entry<String ,Boolean>> getEntrySet() {
        return allNodes.entrySet();
    }

    public int getNumOfAvailableNodes() {
        return count;
    }

}

public class ECSClient implements IECSClient {
    private ZooKeeper zk;
    private CountDownLatch countDownLatch = new CountDownLatch(1);// may be unnecessary
    private HashMap<String, ECSNode> nodeHashMap = new HashMap<>(); // (znodePath, ecsnode)
    private HashMap<String, Process> processHashMap = new HashMap<>(); // (znodePath, processes)
    private String zkIpAddress = "localhost";
    private int zkPort = 3000;
    private int sessionTimeout = 3000;

    private ECSNodes allNodes = new ECSNodes();

    public ECSClient(String zkIpAddress, int zkPort, int sessionTimeout) throws IOException {
        this.zkIpAddress = zkIpAddress;
        this.zkPort = zkPort;
        this.sessionTimeout = sessionTimeout;
        connectToZookeeper();
    }

    public ECSClient() throws IOException {
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
        for (Map.Entry<String, Boolean> entry: allNodes.getEntrySet()) {
            if (entry.getValue() == true) {
                entry.setValue(false);
                String[] temps = entry.getKey().split(":");
                String nodeIP = temps[0];
                int nodePort = Integer.parseInt(temps[1]);
                return new ECSNode(nodeIP, nodePort, cacheSize, cacheStrategy);
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
            nodeHashMap.put(znodePath, node);
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

            allNodes.setInUse("", false);
            System.out.println(e.getLocalizedMessage());
            return null;
        }

        return znodePath;

    }

    private boolean awaitNode(int timeout) {
        // TODO: not sure what to do
        return false;
    }

    //---------------IECSClient Implemntation---------------//
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception {
        return false;
    }

    /**
     * Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean stop() throws Exception {
        return false;
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception {
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
        ECSNode node = nodeHashMap.get(znodePath);
        try {
            startNodeServer(znodePath, node);
        } catch (IOException e) {
            System.out.println("failed to launch node: " + node.getNodeName());
            System.out.println(e.getLocalizedMessage());
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
        // TODO: need to call setupNodes first
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
            nodes.add(nodeHashMap.get(znothPath));
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
        return false;
    }

    /**
     * Get a map of all nodes
     */
    public Map<String, IECSNode> getNodes() {
        return null;
    }

    /**
     * Get the specific node responsible for the given key
     */
    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    //--------------end of IECSClient implementation------------//
}
