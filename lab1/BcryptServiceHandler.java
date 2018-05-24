import java.util.concurrent.CountDownLatch;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.mindrot.jbcrypt.BCrypt;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;

public class BcryptServiceHandler implements BcryptService.Iface {
  private List<BcryptService.AsyncClient> BEClients = new ArrayList<>();
  private BcryptService.AsyncClient FEClient;
  private static List<String> hashedPasswords;
  private static List<Boolean> checkedHashes;
  private static CountDownLatch hashPasswordLatch;
  private static CountDownLatch checkPasswordLatch;

  public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      if (password == null) throw new Exception("list of password is null");
      if (password.size() == 0) throw new Exception("list of password is empty");
      if (logRounds < 4 || logRounds > 31) throw new Exception("logRounds out of range [4,31]");

      hashedPasswords = new ArrayList<>(Collections.nCopies(password.size(), ""));
      int numWorkers = BEClients.size() + 1;
      int numItemsInChunk = password.size() / numWorkers;
      hashPasswordLatch = new CountDownLatch(numWorkers);
      int remainder = password.size() % numWorkers;
      List<String> sublist;
      if (remainder > 0) {
        sublist = password.subList(0, numItemsInChunk + 1);
        remainder--;
      } else {
        sublist = password.subList(0, numItemsInChunk);
      }
      FEClient.hashPasswordBE(sublist, logRounds, new HashPasswordBECallback(0));
      for (int i = 1; i < numWorkers; ++i) {
        BcryptService.AsyncClient client = BEClients.get(i - 1);
        if (remainder > 0) {
          sublist = password.subList(numItemsInChunk * i, (numItemsInChunk * (i + 1)) + 1);
          remainder--;
        } else {
          sublist = password.subList(numItemsInChunk * i, numItemsInChunk * (i + 1));
        }
        client.hashPasswordBE(sublist, logRounds, new HashPasswordBECallback(numItemsInChunk * i));
      }

      System.out.println("hashpassword request sent to BE");
      hashPasswordLatch.await();
      return hashedPasswords;
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
      if (password == null) throw new Exception("list of password is null");
      if (hash == null) throw new Exception("list of hash is null");
      int pwdListSize = password.size();
      int hashListSize = hash.size();
      if (pwdListSize != hashListSize) throw new Exception("password list size: " + pwdListSize + ", hash list size: " + hashListSize);
      if (pwdListSize == 0) throw new Exception("list of password is empty");
      if (hashListSize == 0) throw new Exception("list of hash is empty");
      checkedHashes = new ArrayList<>(Collections.nCopies(password.size(), Boolean.FALSE));
      int numItemsInChunk = password.size() / BEClients.size();
      int remainder = password.size() % BEClients.size();
      checkPasswordLatch = new CountDownLatch(BEClients.size());
      for (int i = 0; i < BEClients.size(); ++i) {
        BcryptService.AsyncClient client = BEClients.get(i);
        List<String> pwdSublist;
        List<String> hashSublist;
        if (remainder > 0) {
          pwdSublist = password.subList(numItemsInChunk * i, (numItemsInChunk * (i + 1)) + 1);
          hashSublist = hash.subList(numItemsInChunk * i, (numItemsInChunk * (i + 1)) + 1);
          remainder--;
        } else {
          pwdSublist = password.subList(numItemsInChunk * i, numItemsInChunk * (i + 1));
          hashSublist = hash.subList(numItemsInChunk * i, numItemsInChunk * (i + 1));
        }
        client.checkPasswordBE(pwdSublist, hashSublist, new CheckPasswordBECallback(numItemsInChunk * i));
      }
      checkPasswordLatch.await();
      return checkedHashes;
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }
  }

  public List<Boolean> checkPasswordBE(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
    try {
      List<Boolean> ret = new ArrayList<>(password.size());
      for (int i = 0; i < password.size(); ++i) {
        String onePwd = password.get(i);
        String oneHash = hash.get(i);
        if (onePwd.equals("") && oneHash.equals("")) {
          ret.add(i, Boolean.TRUE);
        } else {
          ret.add(i, BCrypt.checkpw(onePwd, oneHash));
        }
      }
      return ret;
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }
  }


  public void heartbeatBE(String hostname, short port) throws IllegalArgument, org.apache.thrift.TException {
    try {
      TNonblockingTransport transport = new TNonblockingSocket(hostname, port);
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      TAsyncClientManager clientManager = new TAsyncClientManager();
      BcryptService.AsyncClient BEClient = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
      BEClients.add(BEClient);
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }
  }

  public void initFE(String hostname, short port) throws IllegalArgument, org.apache.thrift.TException {
    try {
      TNonblockingTransport transport = new TNonblockingSocket(hostname, port);
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      TAsyncClientManager clientManager = new TAsyncClientManager();
      FEClient = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
    } catch (Exception e) {
      throw new IllegalArgument(e.getMessage());
    }

  }


  static class HashPasswordBECallback implements AsyncMethodCallback<List<String>> {
    private int startIndex;

    public HashPasswordBECallback (int startIndex) {
      this.startIndex = startIndex;
    }

    public void onComplete(List<String> response) {
      for (int i = 0; i < response.size(); ++i) {
        hashedPasswords.set(this.startIndex + i, response.get(i));
      }
      hashPasswordLatch.countDown();
    }

    public void onError(Exception e) {
      e.printStackTrace();
      hashPasswordLatch.countDown();
    }
  }

  static class CheckPasswordBECallback implements AsyncMethodCallback<List<Boolean>> {
    private int startIndex;

    public CheckPasswordBECallback (int startIndex) {
      this.startIndex = startIndex;
    }

    public void onComplete(List<Boolean> response) {
      for (int i = 0; i < response.size(); ++i) {
        checkedHashes.set(this.startIndex + i, response.get(i));
      }
      checkPasswordLatch.countDown();
    }

    public void onError(Exception e) {
      e.printStackTrace();
      checkPasswordLatch.countDown();
    }
  }
}

