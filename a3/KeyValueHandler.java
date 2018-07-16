import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

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
    private int count;
    private int fail_count;
    private ReadWriteLock lock = new ReentrantReadWriteLock();


    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.count = 0;
        this.fail_count =0;
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();    
    }

    public String get(String key) throws org.apache.thrift.TException
    {   
        lock.readLock().lock();
        String ret = myMap.get(key);
        lock.readLock().unlock();
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        lock.writeLock().lock();
        try{
            myMap.put(key, value);
        }
        catch(Exception e){
            lock.writeLock().unlock();
            fail_count++;
            // System.out.println("something went wrong in put......." + e);
            // System.out.println("fail_count in regular put.........." + fail_count);
            // System.out.println("key : " + key);
            // System.out.println("value : " + value);
        }finally{
            lock.writeLock().unlock();
            put2(key,value);
        }
        // System.out.println("map size : " + myMap.size());
        count++;
    }

    public void put2(String key, String value) throws org.apache.thrift.TException{
        lock.writeLock().lock();
        try{
            List<String> children = curClient.getChildren().forPath(zkNode);
            Collections.sort(children);
            if( children.size() > 1 ){
                byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(1));
                String strData = new String(data);
                String[] primary = strData.split(":");
                TSocket sock = new TSocket(primary[0], Integer.parseInt(primary[1]));
                sock.getSocket().setReuseAddress(true);
                sock.getSocket().setSoLinger(true, 0);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client replication = new KeyValueService.Client(protocol);
                // lock.writelLock().lock();
                replication.putReplica(key, value);
                // lock.writelLock().unlock();
                transport.close();
            }   
            else{
                // System.out.println("There isn't a replication node! : count : " + count);
            }
        } catch (Exception e) {
            lock.writeLock().unlock();
            fail_count++;
            // System.out.println("something went wrong in put......." + e);
            // System.out.println("fail_count......." + fail_count);
            // System.out.println("key : " + key);
            // System.out.println("value : " + value);
        } finally{

            lock.writeLock().unlock();
        }
    }

    public void putReplica(String key, String value) throws org.apache.thrift.TException{
        // lock.lock();
        lock.writeLock().lock();
        try{
            // lock.lock();
            // lock.writelLock().lock();
            myMap.put(key, value);
            // System.out.println("map size : " + myMap.size());
        } catch (Exception e) {
            lock.writeLock().unlock();
            System.out.println("something went wrong in putReplica......." + e);
        } finally{
            lock.writeLock().unlock();
            // lock.unlock();
        }

    }

    public void copySnapshot() throws org.apache.thrift.TException{
        lock.writeLock().lock();
        try{
            System.out.println("************COPY SNAPSHOT*********************************");
            List<String> children = curClient.getChildren().forPath(zkNode);
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
            String strData = new String(data);
            String[] primary = strData.split(":");
            System.out.println("host address " + primary.toString());
            TSocket sock = new TSocket(primary[0], Integer.parseInt(primary[1]));
            sock.getSocket().setReuseAddress(true);
            sock.getSocket().setSoLinger(true, 0);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client primary2 = new KeyValueService.Client(protocol);
            // lock.writeLock().lock();
            myMap = primary2.getSnapshot();
            // lock.writeLock().unlock();
            System.out.println("myMap size from primary : " + myMap.size());
            transport.close();
        } catch(Exception e){
            lock.writeLock().unlock();
            System.out.println("something went wrong in copySnapshot...." + e);
        } finally{
            lock.writeLock().unlock();

        }
    }

    public Map<String, String> getSnapshot() throws org.apache.thrift.TException{
        System.out.println("***********GET SNAPSHOT*********************************");
        System.out.println("myMap size from primary : " + myMap.size());
        return myMap;
    }
}
