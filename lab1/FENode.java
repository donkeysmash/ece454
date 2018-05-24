import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;


public class FENode {
  static Logger log;
  private int port;

  public FENode(int port) {
    this.port = port;
  }

  public static void main(String [] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: java FENode FE_port");
      System.exit(-1);
    }

    // initialize log4j
    BasicConfigurator.configure();
    log = Logger.getLogger(FENode.class.getName());

    int portFE = Integer.parseInt(args[0]);
    FENode node = new FENode(portFE);
    log.info("Launching FE node on port " + portFE);

    // launch Thrift server
    BcryptService.Processor processor = new BcryptService.Processor(new BcryptServiceHandler());
    TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
    THsHaServer.Args sargs = new THsHaServer.Args(socket);
    sargs.protocolFactory(new TBinaryProtocol.Factory());
    sargs.transportFactory(new TFramedTransport.Factory());
    sargs.processorFactory(new TProcessorFactory(processor));
    sargs.maxWorkerThreads(5);
    TServer server = new THsHaServer(sargs);
    node.initFE();
    server.serve();
  }

  public void initFE() throws Exception {
    TSocket sock = new TSocket(getHostName(), this.port);
    TTransport transport = new TFramedTransport(sock);
    TProtocol protocol = new TBinaryProtocol(transport);
    BcryptService.Client client = new BcryptService.Client(protocol);
    transport.open();
    client.initFE(getHostName(), (short) this.port);
    transport.close();
  }

  static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "localhost";
    }
  }
}
