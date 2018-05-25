import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;


public class BENode {
  static Logger log;
  private String hostFE;
  private int portBE;
  private int portFE;
  private TTransport FEtransport;
  private BcryptService.Client FEclient;

  public BENode(String hostFE, int portFE, int portBE) {
    this.hostFE = hostFE;
    this.portFE = portFE;
    this.portBE = portBE;
    TSocket sock = new TSocket(hostFE, portFE);
    this.FEtransport = new TFramedTransport(sock);
    TProtocol protocol = new TBinaryProtocol(FEtransport);
    this.FEclient = new BcryptService.Client(protocol);
  }

  public static void main(String [] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: java BENode FE_host FE_port BE_port");
      System.exit(-1);
    }

    // initialize log4j
    BasicConfigurator.configure();
    log = Logger.getLogger(BENode.class.getName());

    String hostFE = args[0];
    int portFE = Integer.parseInt(args[1]);
    int portBE = Integer.parseInt(args[2]);
    log.info("Launching BE node on port " + portBE + " at host " + getHostName());
    BENode node = new BENode(hostFE, portFE, portBE);

    // launch Thrift server
    BcryptService.Processor processor = new BcryptService.Processor(new BcryptServiceHandler());
    TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
    //TServerSocket socket = new TServerSocket(portBE);
    THsHaServer.Args sargs = new THsHaServer.Args(socket);
    sargs.protocolFactory(new TBinaryProtocol.Factory());
    sargs.transportFactory(new TFramedTransport.Factory());
    sargs.processorFactory(new TProcessorFactory(processor));
    sargs.maxWorkerThreads(64);
    THsHaServer server = new THsHaServer(sargs);
    node.pingFE();
    server.serve();
  }

  public void pingFE() throws Exception {
    this.FEtransport.open();
    this.FEclient.heartbeatBE(getHostName(), (short)this.portBE);
    this.FEtransport.close();
  }

  static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "localhost";
    }
  }
}

