import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client {
  public static void main(String [] args) {
    if (args.length != 2) {
      System.err.println("Usage: java Client FE_host FE_port");
      System.exit(-1);
    }

    try {
      TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
      TTransport transport = new TFramedTransport(sock);
      TProtocol protocol = new TBinaryProtocol(transport);
      BcryptService.Client client = new BcryptService.Client(protocol);
      List<String> passwords = new ArrayList<>();
      for (int i = 0; i < 10; ++i) {
        passwords.add("sompaomdfspofm" + i);
      }
      passwords.add("");
      transport.open();
      System.out.println("testing started         -- size: " + passwords.size());
      List<String> hashed = client.hashPassword(passwords, (short)4);
      System.out.println("hashPassword completed  -- size: " + hashed.size());
      for (String x : hashed) {
        System.out.println(x);
      }
      hashed.set(0, "somebadstringthatisntevenhash");
      List<Boolean> checked = client.checkPassword(passwords, hashed);
      System.out.println("chekcPassword completed -- size: " + hashed.size());
      for (boolean x : checked) {
        System.out.println(x);
      }

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }
  }
}

