import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;

public class BcryptServiceHandler implements BcryptService.Iface {
  private List<BcryptService.Client> BEClients = new ArrayList<>();
  private List<TTransport> BETransports = new ArrayList<>();

  public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      List<String> ret = new ArrayList<>(password.size());
      int numItemsInChunk = password.size() / BEClients.size();
      for (int i = 0; i < BEClients.size(); ++i) {
        BcryptService.Client client = BEClients.get(i);
        TTransport transport = BETransports.get(i);
        transport.open();
        List<String> sublist = password.subList(numItemsInChunk * i, numItemsInChunk * (i + 1));
        List<String> returnedFromBE = client.hashPasswordBE(sublist, logRounds);
        transport.close();
        ret.addAll(returnedFromBE);
      }
      return ret;
    } catch (Exception e) {
      System.out.println(e.getMessage());
      throw new IllegalArgument(e.getMessage());
    }
  }

  public List<String> hashPasswordBE(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      List<String> ret = new ArrayList<>(password.size());
      for (String x : password) {
        String hashedPwd = BCrypt.hashpw(x, BCrypt.gensalt(logRounds));
        ret.add(hashedPwd);
      }
      return ret;
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }
  }

  public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
    try {
      int pwdListSize = password.size();
      int hashListSize = hash.size();
      if (pwdListSize != hashListSize) {
        throw new Exception("password list size: " + pwdListSize + ", hash list size: " + hashListSize);
      }
      List<Boolean> ret = new ArrayList<>(pwdListSize);
      for (int i = 0; i < pwdListSize; ++i) {
        String onePwd = password.get(i);
        String oneHash = hash.get(i);
        ret.add(i, BCrypt.checkpw(onePwd, oneHash));
      }
      return ret;
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }
  }

  public void ping(String hostname, short portBE) {
    TSocket sock = new TSocket(hostname, portBE);
    TTransport BETransport = new TFramedTransport(sock);
    TProtocol protocol = new TBinaryProtocol(BETransport);
    BcryptService.Client BEClient = new BcryptService.Client(protocol);
    BETransports.add(BETransport);
    BEClients.add(BEClient);
  }
}

