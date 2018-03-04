package com.company;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;

/**
 * Created by tianqiliu on 2018-03-03.
 */
public class ECSClient implements IECSClient {
    private ZooKeeper zk;
    private CountDownLatch countDownLatch = new CountDownLatch(1);// may be unnecessary
    private HashMap<String, ECSNode> nodeHashMap = new HashMap<>();

    private String ipAddress = "localhost:3000";
    private int port = 3000;
    private int timeOut = 3000;

    public ECSClient(String ipAddress, int port, int timeOut) throws IOException {
        this.ipAddress = ipAddress;
        this.port = port;
        this.timeOut = timeOut;
    }

    //------------------custom implementation---------------//
    private void init() throws IOException {
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
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO: logging in exceptions
        ECSNode node = new ECSNode(ipAddress, port, cacheSize, cacheStrategy);
        try {
            byte[] bytes = node.toBytes();
            String nodePath = zk.create(node.getNodeHash(), bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            nodeHashMap.put(nodePath, node);
            // TODO: start the server associated with node
            // TODO: moving data of affected servers (read requests can be served)
            // TODO: update metedata of all servers
        } catch (IOException e) {
            // node cannot be converted to byte array
            System.out.println("failed to convert node into byte array");
            return null;
        } catch (KeeperException.InvalidACLException e) {
            System.out.println("the ACL is invalid, null, or empty");
            return null;
        } catch (KeeperException e) {
            System.out.println("the zookeeper server returns a non-zero error code");
            return null;
        } catch (InterruptedException e) {
            System.out.println(e.getLocalizedMessage());
            System.out.println("exiting client due to interrupted exception");
            System.exit(-1);
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
        return null;
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
