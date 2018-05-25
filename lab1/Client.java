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
    if (args.length != 4) {
      System.err.println("Usage: java Client FE_host FE_port length logrounds");
      System.exit(-1);
    }

    try {
      TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
      TTransport transport = new TFramedTransport(sock);
      TProtocol protocol = new TBinaryProtocol(transport);
      BcryptService.Client client = new BcryptService.Client(protocol);

      int lengthIwant = Integer.parseInt(args[2]);
      short logRoundsIwant = Short.parseShort(args[3]);

      List<String> passwords = new ArrayList<>(lengthIwant);
      for (int i = 0; i < lengthIwant; ++i) {
        passwords.add("sompaomdfspofm" + i);
      }
      passwords.add("");
      transport.open();
      System.out.println("testing started         -- size: " + passwords.size());
      System.out.println("                        -- logR: " + logRoundsIwant);
      System.out.println();
      long startTime = System.nanoTime();
      List<String> hashed = client.hashPassword(passwords, logRoundsIwant);
      long endTime = System.nanoTime();
      long duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
      System.out.println("hashPassword completed  -- size: " + hashed.size());
      System.out.println("                        -- time: " + duration + " ms");
      System.out.println();
      hashed.set(0, "somebadstringthatisntevenhash");
      startTime = System.nanoTime();
      List<Boolean> checked = client.checkPassword(passwords, hashed);
      endTime = System.nanoTime();
      duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
      System.out.println("chekcPassword completed -- size: " + hashed.size());
      System.out.println("                        -- time: " + duration + " ms");
      System.out.println();
      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }
  }
}

