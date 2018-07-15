import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
  static Logger log;

  public static void main(String [] args) throws Exception {
    BasicConfigurator.configure();
    log = Logger.getLogger(StorageNode.class.getName());

    if (args.length != 4) {
      System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
      System.exit(-1);
    }

    CuratorFramework curClient =
      CuratorFrameworkFactory.builder()
      .connectString(args[2])
      .retryPolicy(new RetryNTimes(10, 1000))
      .connectionTimeoutMs(1000)
      .sessionTimeoutMs(10000)
      .build();

    curClient.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        curClient.close();
      }
    });

    KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
    TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
    TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
    sargs.protocolFactory(new TBinaryProtocol.Factory());
    sargs.transportFactory(new TFramedTransport.Factory());
    sargs.processorFactory(new TProcessorFactory(processor));
    sargs.maxWorkerThreads(64);
    TServer server = new TThreadPoolServer(sargs);
    log.info("Launching server");

    new Thread(new Runnable() {
      public void run() {
        server.serve();
      }
    }).start();

    String ipAddress = args[0] + ":" + args[1];
    byte[] payload = ipAddress.getBytes();
    String zkNode = args[3];
    curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/child", payload);
    List<String> children = curClient.getChildren().forPath(zkNode);
    Collections.sort(children);
    byte[] payloadPrimary = curClient.getData().forPath(zkNode + "/" + children.get(0));
    String ipAddressPrimary = new String(payloadPrimary);

    log.info("numServers: " + children.size());
    log.info("children: " + String.join(", ",  children));
    if (children.size() > 1) { // must create replicate
      log.info("more than 1 node so let's replicate");
      for (String child : children) {
        String ipAddressChild = new String(curClient.getData().forPath(zkNode + "/" + child));
        log.info("Child name : " + child + "@" + ipAddressChild);
        if (!ipAddressChild.equals(ipAddressPrimary)) {
          log.info("new backup - start replicate");
          String[] ipAddressFromArr = ipAddressPrimary.split(":");
          TSocket sockFrom = new TSocket(ipAddressFromArr[0], Integer.parseInt(ipAddressFromArr[1]));
          TTransport transport = new TFramedTransport(sockFrom);
          transport.open();
          TProtocol protocol = new TBinaryProtocol(transport);
          KeyValueService.Client clientBackup = new KeyValueService.Client(protocol);
          Map<String, String> data = clientBackup.getData();
          log.info("retrieved " + data.size() + " records");
          transport.close();

          String[] ipAddressToArr = ipAddress.split(":");
          TSocket sockTo = new TSocket(ipAddressToArr[0], Integer.parseInt(ipAddressToArr[1]));
          TTransport transport2 = new TFramedTransport(sockTo);
          transport2.open();
          TProtocol protocol2 = new TBinaryProtocol(transport2);
          KeyValueService.Client clientPrimary = new KeyValueService.Client(protocol2);
          log.info("copying data");
          clientPrimary.copyData(data);
        } else {
          log.info("this is primary so skip");
        }
      }

    } //needs replication
    log.info("init completed");
  }
}
