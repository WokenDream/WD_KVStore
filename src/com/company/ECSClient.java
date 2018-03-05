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
    private String ipAddress = "localhost:3000";
    private int port = 3000;
    private int timeOut = 3000;

//    private HashMap<String, Boolean> allNodes = new HashMap<>(); // (node's ip + port, launched or not)
    private ECSNodes allNodes = new ECSNodes();

    public ECSClient(String ipAddress, int port, int timeOut) throws IOException {
        this.ipAddress = ipAddress;
        this.port = port;
        this.timeOut = timeOut;
        connectToZookeeper();
    }

    public ECSClient() throws IOException {
        connectToZookeeper();
    }

    //------------------custom implementation---------------//
    private void connectToZookeeper() throws IOException {
        zk = new ZooKeeper("host", timeOut, new Watcher() {
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

    private void startNodeServer(String znodePath, IECSNode node) throws IOException {
        // TODO: complete path and argument of server
        String cmd = "ssh -n " + ipAddress + " nohup java -jar <path>/ms2-server.jar " + port + "blabla";
        Process process = Runtime.getRuntime().exec(cmd);
        processHashMap.put(znodePath, process);
    }

    /**
     * find an available node and mark the found node as in use (false)
     * @param cacheStrategy
     * @param cacheSize
     * @return
     */
    private ECSNode getAvailableNode(String cacheStrategy, int cacheSize) {
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
        ECSNode node = getAvailableNode(cacheStrategy, cacheSize);
        if (node == null) {
            return null;
        }
        try {
            byte[] bytes = node.toBytes();
            String znodePath = zk.create(node.getNodeHash(), bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            nodeHashMap.put(znodePath, node);
            startNodeServer(znodePath, node);
            // TODO: moving data of affected servers (read requests can be served)
            // TODO: update metadata of all servers
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
        return null;
    }

    /**
     * Wait for all nodes to report status or until timeout expires
     * @param count     number of nodes to wait for
     * @param timeout   the timeout in milliseconds
     * @return  true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
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
