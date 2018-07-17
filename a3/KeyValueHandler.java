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
import org.apache.curator.framework.api.*;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
  private Map<String, String> myMap;
  private CuratorFramework curClient;
  private String zkNode;
  private String host;
  private int port;
  private String primaryAddress;
  private Map<String, Boolean> backupAddresses;
  private boolean isPrimary;

  private Set<String> keyLocks = new HashSet<>();

  public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
    this.host = host;
    this.port = port;
    this.curClient = curClient;
    this.zkNode = zkNode;
    this.myMap = new ConcurrentHashMap<String, String>();
    this.backupAddresses = new HashMap<>();
    try {
      this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }

  public String get(String key) throws org.apache.thrift.TException {
    if (this.isPrimary) {
      try {
        this.lockKey(key);
        String ret = myMap.get(key);
        if (ret == null) {
          return "";
        }
        return ret;
      } catch (Exception e) {
        //System.out.println("get error " + e.toString());
      } finally {
        this.unlockKey(key);
      }
    } else {
      throw new TException();
    }
    return "";
  }

  public void put(String key, String value) throws org.apache.thrift.TException {
    try {
      this.lockKey(key);
      myMap.put(key, value);
      if (this.backupAddresses.size() > 0) {
        for (String backupAddress : this.backupAddresses.keySet()) {
          String[] splited = backupAddress.split(":");
          TSocket s = new TSocket(splited[0], Integer.parseInt(splited[1]));
          TTransport t = new TFramedTransport(s);
          TProtocol p = new TBinaryProtocol(t);
          t.open();
          KeyValueService.Client backupClient = new KeyValueService.Client(p);
          backupClient.putBackup(key, value);
          t.close();
        }
      }
    } catch (Exception e) {
      //System.out.println("Error in put " + e.toString());
    } finally {
      this.unlockKey(key);
    }
  }

  public void putBackup(String key, String value) throws org.apache.thrift.TException {
    if (this.isPrimary) {
      throw new TException();
    } else {
      try {
        myMap.put(key, value);
      } catch (Exception e) {
        //System.out.println("Error in putBackup" + e.toString());
      }
    }

  }

  public void copyMap(Map<String, String> input) throws org.apache.thrift.TException {
    myMap.putAll(input);
    System.out.println("copyMap -- size: " + input.size());
  }

  synchronized private void replicateData() {
    if (this.isPrimary) {
      for (String backupAddress : this.backupAddresses.keySet()) {
        if (!this.backupAddresses.get(backupAddress)) {
          System.out.println("First time seeing backup " + backupAddress + " - try replicate");
          try {
            String[] splited = backupAddress.split(":");
            this.backupAddresses.put(backupAddress, true);
            TSocket s = new TSocket(splited[0], Integer.parseInt(splited[1]));
            TTransport t = new TFramedTransport(s);
            TProtocol p = new TBinaryProtocol(t);
            t.open();
            KeyValueService.Client backupClient = new KeyValueService.Client(p);
            backupClient.copyMap(myMap);
            t.close();
          } catch (Exception e) {
            //System.out.println("replicate data ERROR " + e.toString());
          }
        } else {
          System.out.println("already replicated " + backupAddress);
        }
      }
    }
  }

  private void unlockKey(String key) {
    synchronized (this.keyLocks) {
      this.keyLocks.remove(key);
      this.keyLocks.notifyAll();
    }
  }

  private void lockKey(String key) throws Exception {
    synchronized (this.keyLocks) {
      while (this.keyLocks.contains(key)) {
        this.keyLocks.wait();
      }
      this.keyLocks.add(key);
    }
  }


  private List<String> getZKChildren() throws Exception {
    while (true) {
      this.curClient.sync();
      List<String> children =
        this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);
      if (children.size() == 0) {
        System.out.println("No children found");
        Thread.sleep(100);
        continue;
      }
      Collections.sort(children);
      return children;
    }
  }

  private void updatePrimaryAddress(List<String> zks) throws Exception {
    byte[] data = this.curClient.getData().forPath(this.zkNode + "/" + zks.get(0));
    this.primaryAddress = new String(data);
  }
  private void updateBackupAddresses(List<String> zks) throws Exception {
    this.backupAddresses.remove(this.primaryAddress);
    for (int i = 1; i < zks.size(); ++i) {
      byte[] data = this.curClient.getData().forPath(this.zkNode + "/" + zks.get(i));
      String backupAddress = new String(data);
      if (!this.backupAddresses.containsKey(backupAddress)) {
        this.backupAddresses.put(backupAddress, false);
      }
    }
  }

  synchronized public void process(WatchedEvent event) {
    System.out.println("ZooKeeper event " + event);
    try {
      List<String> zkChildren = getZKChildren();
      System.out.println("All current " + zkChildren.size() + " children: " + String.join(", ", zkChildren));
      this.updatePrimaryAddress(zkChildren);
      this.updateBackupAddresses(zkChildren);
      this.isPrimary = (this.host + ":" + this.port).equals(this.primaryAddress);
      if (this.isPrimary) {
        System.out.println("THIS IS PRIMARY NOW");
      } else {
        System.out.println("SECONDARY SERVER");
      }
      this.replicateData();
    } catch (Exception e) {
      System.out.println("Unable to determine primary " + e.toString());
    }
  }

}
