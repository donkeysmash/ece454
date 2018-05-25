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
  private static CountDownLatch hashPasswordLatch;
  private static CountDownLatch checkPasswordLatch;

  public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      if (password == null) throw new Exception("list of password is null");
      if (password.size() == 0) throw new Exception("list of password is empty");
      if (logRounds < 4 || logRounds > 31) throw new Exception("logRounds out of range [4,31]");

      List<String> hashedPasswords = new ArrayList<>(Collections.nCopies(password.size(), ""));
      int numWorkers = BEClients.size() + 1;
      int numItemsInChunk = password.size() / numWorkers;
      hashPasswordLatch = new CountDownLatch(numWorkers);
      int remainder = password.size() % numWorkers;
      List<String> sublist;
      int chunkStartIdx = 0;
      int chunkEndIdx = remainder > 0 ? numItemsInChunk + 1 : numItemsInChunk;
      remainder--;
      for (BcryptService.AsyncClient client : BEClients) {
        sublist = password.subList(chunkStartIdx, chunkEndIdx);
        client.hashPasswordBE(sublist, logRounds, new HashPasswordBECallback(hashedPasswords, chunkStartIdx));
        chunkStartIdx = chunkEndIdx;
        chunkEndIdx = remainder > 0 ? chunkStartIdx + numItemsInChunk + 1 : chunkStartIdx + numItemsInChunk;
        remainder--;
      }
      sublist = password.subList(chunkStartIdx, password.size());
      List<String> FEResult = this.hashPasswordBE(sublist, logRounds);
      for (int i = 0; i < FEResult.size(); ++i) {
        hashedPasswords.set(chunkStartIdx + i, FEResult.get(i));
      }
      hashPasswordLatch.countDown();
      hashPasswordLatch.await();
      return hashedPasswords;
    } catch (Exception e) {
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
      List<Boolean> checkedHashes = new ArrayList<>(Collections.nCopies(password.size(), Boolean.FALSE));
      int numWorkers = BEClients.size() + 1;
      int numItemsInChunk = password.size() / numWorkers;
      checkPasswordLatch = new CountDownLatch(numWorkers);
      int remainder = password.size() % numWorkers;
      List<String> pwdSublist;
      List<String> hashSublist;
      int chunkStartIdx = 0;
      int chunkEndIdx = remainder > 0 ? numItemsInChunk + 1 : numItemsInChunk;
      remainder--;
      for (BcryptService.AsyncClient client : BEClients) {
        pwdSublist = password.subList(chunkStartIdx, chunkEndIdx);
        hashSublist = hash.subList(chunkStartIdx, chunkEndIdx);
        client.checkPasswordBE(pwdSublist, hashSublist, new CheckPasswordBECallback(checkedHashes, chunkStartIdx));
        chunkStartIdx = chunkEndIdx;
        chunkEndIdx = remainder > 0 ? chunkStartIdx + numItemsInChunk + 1 : chunkStartIdx + numItemsInChunk;
        remainder--;
      }
      pwdSublist = password.subList(chunkStartIdx, password.size());
      hashSublist = hash.subList(chunkStartIdx, hash.size());
      List<Boolean> FEResult = this.checkPasswordBE(pwdSublist, hashSublist);
      for (int i = 0; i < FEResult.size(); ++i) {
        checkedHashes.set(i + chunkStartIdx, FEResult.get(i));
      }
      checkPasswordLatch.countDown();
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
          boolean result = false;
          try {
            result = BCrypt.checkpw(onePwd, oneHash);
          } catch (Exception e) {
            // do nothing
          }
          ret.add(i, result);
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

  static class HashPasswordBECallback implements AsyncMethodCallback<List<String>> {
    private int startIndex;
    private List<String> hashedPasswords;

    public HashPasswordBECallback (List<String> hashedPasswords, int startIndex) {
      this.startIndex = startIndex;
      this.hashedPasswords = hashedPasswords;
    }

    public void onComplete(List<String> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.hashedPasswords.set(this.startIndex + i, response.get(i));
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
    private List<Boolean> checkedHashes;

    public CheckPasswordBECallback (List<Boolean> checkedHashes, int startIndex) {
      this.startIndex = startIndex;
      this.checkedHashes = checkedHashes;
    }

    public void onComplete(List<Boolean> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.checkedHashes.set(this.startIndex + i, response.get(i));
      }
      checkPasswordLatch.countDown();
    }

    public void onError(Exception e) {
      e.printStackTrace();
      checkPasswordLatch.countDown();
    }
  }
}

