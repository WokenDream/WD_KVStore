package com.company;
import java.util.Collection;
import java.util.Map;

/**
 * Created by tianqiliu on 2018-01-16.
 */
interface IKVServer {
    enum CacheStrategy {
        None, FIFO, LRU, LFU
    }
}


class CacheNode {
    String key;
    int freq = 1;

    public CacheNode(String key) {
        this.key = key;
    }

    public CacheNode(String key, int freq) {
        this.key = key;
        this.freq = freq;
    }

    public boolean equals(Object obj) {
        return (obj instanceof CacheNode) && ((CacheNode)obj).key.equals(this.key);
    }
}

interface IECSNode {

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

}

interface IECSClient {
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception;

    /**
     * Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean stop() throws Exception;

    /**
     * Stops all server instances and exits the remote processes.
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean shutdown() throws Exception;

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     */
    public IECSNode addNode(String cacheStrategy, int cacheSize);

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     * @return  set of strings containing the names of the nodes
     */
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize);

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * @return  array of strings, containing unique names of servers
     */
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize);

    /**
     * Wait for all nodes to report status or until timeout expires
     * @param count     number of nodes to wait for
     * @param timeout   the timeout in milliseconds
     * @return  true if all nodes reported successfully, false otherwise
     */
    public boolean awaitNodes(int count, int timeout) throws Exception;

    /**
     * Removes nodes with names matching the nodeNames array
     * @param nodeNames names of nodes to remove
     * @return  true on success, false otherwise
     */
    public boolean removeNodes(Collection<String> nodeNames);

    /**
     * Get a map of all nodes
     */
    public Map<String, IECSNode> getNodes();

    /**
     * Get the specific node responsible for the given key
     */
    public IECSNode getNodeByKey(String Key);
}

class KVStorageResult {

    public enum ResultType {
        PUT_SUCCESS,
        PUT_ERROR,
        PUT_UPDATE_SUCCESS,
        PUT_UPDATE_ERROR,
        DELETE_SUCCESS,
        DELETE_ERROR,
        GET_SUCCESS,
        GET_ERROR
    }

    private ResultType result;
    private String value;

    public KVStorageResult() {
        result = null;
        value = null;
    }

    public KVStorageResult (ResultType result) {
        this.result = result;
    }

    public KVStorageResult (ResultType result, String value) {
        this.result = result;
        this.value = value;
    }

    public void setResult(ResultType result) {
        this.result = result;
    }

    public ResultType getResult() {
        return this.result;
    }
    public void setValue(String value) {
        this.value = value;
    }
    public String getValue() {
        return this.value;
    }
}
