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

		//concat ip and port for this node(server)
		String hostPort = args[0]+":"+args[1];
		byte[] dataIP = hostPort.getBytes();
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/a", dataIP);

		// TODO: create an ephemeral node in ZooKeeper
		System.out.println("RUNNING...");


		List<String> children = curClient.getChildren().forPath(args[3]);
		Collections.sort(children);
		byte[] data = curClient.getData().forPath(args[3] + "/" + children.get(0));	


		//determines if primary
		boolean primary = Arrays.equals(data, dataIP);
		System.out.println("current is primary : " + primary);	

		for(String child: children){
			System.out.println("child : " + child);
		}

		// if back-up, do the replication process
			if(!primary){
				List<String> children1 = curClient.getChildren().forPath(args[3]);
				Collections.sort(children1);
				byte[] data1 = curClient.getData().forPath(args[3] + "/" + children.get(1));
				String strData = new String(data1);
	        	String[] primary1 = strData.split(":");
				TSocket sock = new TSocket(primary1[0], Integer.parseInt(primary1[1]));
		        TTransport transport = new TFramedTransport(sock);
		        transport.open();
		        TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client primary2 = new KeyValueService.Client(protocol);
				primary2.copySnapshot();
			}
    }
}
