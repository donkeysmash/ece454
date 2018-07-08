import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private KeyValueService.Client clientBackup;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
    	this.host = host;
    	this.port = port;
    	this.curClient = curClient;
    	this.zkNode = zkNode;
    	this.myMap = new ConcurrentHashMap<String, String>();

        //decide who's primary here	
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
    	String ret = myMap.get(key);
    	if (ret == null)
    	    return "";
    	else
    	    return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
	    myMap.put(key, value);
        List<String> children = curClient.getChildren().forPath(zkNode);
        if (children.size == 1) {
            clientBackup = null
        } else if (children.size() > 1 && clientBackup != null) {
            clientBackup.putBackup(key, value);
        } else {
            initBackUpClient();
            clientBackup.putBackup(key, value);
        }
    }

    public void putBackup(String key, String value) throws org.apache.thrift.TException
    {
        lock.writeLock().lock();
        try{
            myMap.put(key, value);
        } catch (Exception e) {
            lock.writeLock().unlock();
        } finally{
            lock.writeLock().unlock();
        }
    }

    public void initBackUpClient() {
      
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        if (children.size() > 1) {
            Collections.sort(children);
            byte[] payloadPrimary = curClient.getData().forPath(zkNode + "/" + children.get(0));    
            String ipAddressPrimary = new String(payloadPrimary);
            String[] ipAddressPrimaryArray = ipAddressPrimary.split(":");
            TSocket sock = new TSocket(ipAddressPrimaryArray[0], Integer.parseInt(ipAddressPrimaryArray[1]));
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            clientBackup = KeyValueService.Client(protocol);
        } 
    }

    public void copyData(data) throws org.apache.thrift.TException{
        myMap = data;
    }

    public Map<String, String> getData() throws org.apache.thrift.TException{
        return myMap;
    }

}
