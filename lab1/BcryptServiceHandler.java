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
  static private Set<SocketInfo> BEsockets = new HashSet<>();

  public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      System.out.println("[hashPassword] request received with size: " + password.size());
      if (password == null) throw new Exception("list of password is null");
      if (password.size() == 0) throw new Exception("list of password is empty");
      if (logRounds < 4 || logRounds > 31) throw new Exception("logRounds out of range [4,31]");
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      TAsyncClientManager clientManager = new TAsyncClientManager();
      List<String> hashedPasswords = new ArrayList<>(Collections.nCopies(password.size(), ""));
      int numWorkers = BEsockets.size() + 1;
      int numItemsInChunk = password.size() / numWorkers;
      CountDownLatch hashPasswordLatch = new CountDownLatch(numWorkers);
      int remainder = password.size() % numWorkers;
      List<String> sublist;
      int chunkStartIdx = 0;
      int chunkEndIdx = remainder > 0 ? numItemsInChunk + 1 : numItemsInChunk;
      remainder--;
      for (SocketInfo socket : BEsockets) {
        System.out.println("[hashPassword] trying to reach BENode at " + socket.toString());
        TNonblockingTransport transport = new TNonblockingSocket(socket.getHostname(), socket.getPort());
        BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
        sublist = password.subList(chunkStartIdx, chunkEndIdx);
        System.out.println("                           size of work: " + sublist.size());
        client.hashPasswordBE(sublist, logRounds, new HashPasswordBECallback(this, sublist, logRounds, socket, transport, hashPasswordLatch, hashedPasswords, chunkStartIdx));
        chunkStartIdx = chunkEndIdx;
        chunkEndIdx = remainder > 0 ? chunkStartIdx + numItemsInChunk + 1 : chunkStartIdx + numItemsInChunk;
        remainder--;
      }
      sublist = password.subList(chunkStartIdx, password.size());
      System.out.println("[hashPassword] giving some work for FE size: " + sublist.size());
      List<String> FEResult = this.hashPasswordBE(sublist, logRounds);
      for (int i = 0; i < FEResult.size(); ++i) {
        hashedPasswords.set(chunkStartIdx + i, FEResult.get(i));
      }
      hashPasswordLatch.countDown();
      hashPasswordLatch.await();
      System.out.println("[hashPassword] successful with size " + hashedPasswords.size() + "\n");
      return hashedPasswords;
    } catch (Exception e) {
      System.out.println("[hashPassword-exception] " + e.getMessage());
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
      System.out.println("[hashPasswordBE] " + e.getMessage());
      throw new IllegalArgument(e.getMessage());
    }
  }

  public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
    try {
      System.out.println("[checkPassword] request received with size: " + password.size());
      if (password == null) throw new Exception("list of password is null");
      if (hash == null) throw new Exception("list of hash is null");
      int pwdListSize = password.size();
      int hashListSize = hash.size();
      if (pwdListSize != hashListSize) throw new Exception("password list size: " + pwdListSize + ", hash list size: " + hashListSize);
      if (pwdListSize == 0) throw new Exception("list of password is empty");
      if (hashListSize == 0) throw new Exception("list of hash is empty");
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      TAsyncClientManager clientManager = new TAsyncClientManager();
      List<Boolean> checkedHashes = new ArrayList<>(Collections.nCopies(password.size(), Boolean.FALSE));
      int numWorkers = BEsockets.size() + 1;
      int numItemsInChunk = password.size() / numWorkers;
      CountDownLatch checkPasswordLatch = new CountDownLatch(numWorkers);
      int remainder = password.size() % numWorkers;
      List<String> pwdSublist;
      List<String> hashSublist;
      int chunkStartIdx = 0;
      int chunkEndIdx = remainder > 0 ? numItemsInChunk + 1 : numItemsInChunk;
      remainder--;
      for (SocketInfo socket : BEsockets) {
        System.out.println("[checkPassword] trying to reach BENode at " + socket.toString());
        TNonblockingTransport transport = new TNonblockingSocket(socket.getHostname(), socket.getPort());
        BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
        pwdSublist = password.subList(chunkStartIdx, chunkEndIdx);
        System.out.println("                            size of work: " + pwdSublist.size());
        hashSublist = hash.subList(chunkStartIdx, chunkEndIdx);
        client.checkPasswordBE(pwdSublist, hashSublist, new CheckPasswordBECallback(this, pwdSublist, hashSublist, socket, transport, checkPasswordLatch, checkedHashes, chunkStartIdx));
        chunkStartIdx = chunkEndIdx;
        chunkEndIdx = remainder > 0 ? chunkStartIdx + numItemsInChunk + 1 : chunkStartIdx + numItemsInChunk;
        remainder--;
      }
      pwdSublist = password.subList(chunkStartIdx, password.size());
      hashSublist = hash.subList(chunkStartIdx, hash.size());
      System.out.println("[checkPassword] giving some work for FE size: " + pwdSublist.size());
      List<Boolean> FEResult = this.checkPasswordBE(pwdSublist, hashSublist);
      for (int i = 0; i < FEResult.size(); ++i) {
        checkedHashes.set(i + chunkStartIdx, FEResult.get(i));
      }
      checkPasswordLatch.countDown();
      checkPasswordLatch.await();
      System.out.println("[checkPassword] successful with size " + checkedHashes.size() + "\n");
      return checkedHashes;
    } catch (Exception e) {
      System.out.println("[checkPassword-exception] " + e.getMessage());
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
      System.out.println("[checkPasswordBE] " + e.getMessage());
      throw new IllegalArgument(e.getMessage());
    }
  }


  public void heartbeatBE(String hostname, short port) throws IllegalArgument, org.apache.thrift.TException {
    try {
      BEsockets.add(new SocketInfo(hostname, port));
    } catch (Exception e) {
      System.out.println("[heartbeatBE] " + e.getMessage());
      throw new IllegalArgument(e.getMessage());
    }
  }

  class SocketInfo {
    private short port;
    private String hostname;
    public SocketInfo(String hostname, short port) {
      this.hostname = hostname;
      this.port = port;
    }
    public String getHostname() { return this.hostname; }
    public short getPort() { return this.port; }

    @Override
    public boolean equals(Object other) {
      if (other == null) return false;
      if (other == this) return true;
      if (!(other instanceof SocketInfo)) return false;
      SocketInfo otherInClass = (SocketInfo) other;
      return otherInClass.getHostname().equals(this.hostname) && otherInClass.getPort() == this.port;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.port, this.hostname);
    }

    @Override
    public String toString() {
      return this.hostname + ":" + this.port;
    }
  }

  static class HashPasswordBECallback implements AsyncMethodCallback<List<String>> {
    private int startIndex;
    private List<String> hashedPasswords;
    private List<String> original;
    private short logRounds;
    private CountDownLatch hashPasswordLatch;
    private TNonblockingTransport transport;
    private SocketInfo socketInfo;
    private BcryptServiceHandler handler;

    public HashPasswordBECallback (BcryptServiceHandler handler, List<String> original, short logRounds, SocketInfo socketInfo, TNonblockingTransport transport, CountDownLatch hashPasswordLatch, List<String> hashedPasswords, int startIndex) {
      this.startIndex = startIndex;
      this.hashedPasswords = hashedPasswords;
      this.hashPasswordLatch = hashPasswordLatch;
      this.socketInfo = socketInfo;
      this.transport = transport;
      this.original = original;
      this.logRounds = logRounds;
      this.handler = handler;
    }

    public void onComplete(List<String> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.hashedPasswords.set(this.startIndex + i, response.get(i));
      }
      this.transport.close();
      this.hashPasswordLatch.countDown();
    }

    public void onError(Exception e) {
      try {
        System.out.println("[hassPasswordCB onError, trying redistribution] " + e.getMessage());
        System.out.println("                                              @ " + this.socketInfo.toString());
        this.transport.close();
        BEsockets.remove(this.socketInfo);
        List<String> response = this.handler.hashPassword(this.original, this.logRounds);
        for (int i = 0; i < response.size(); ++i) {
          this.hashedPasswords.set(this.startIndex + i, response.get(i));
        }
      } catch (Exception ex) {
        System.out.println("[hashPasswordCB - redistributing Exception] " + ex.getMessage());
      } finally {
        this.hashPasswordLatch.countDown();
      }
    }
  }

  static class CheckPasswordBECallback implements AsyncMethodCallback<List<Boolean>> {
    private int startIndex;
    private List<Boolean> checkedHashes;
    private CountDownLatch checkPasswordLatch;
    private TNonblockingTransport transport;
    private BcryptServiceHandler handler;
    private List<String> passwords;
    private List<String> hashes;
    private SocketInfo socketInfo;

    public CheckPasswordBECallback (BcryptServiceHandler handler, List<String> passwords, List<String> hashes, SocketInfo socketInfo, TNonblockingTransport transport, CountDownLatch checkPasswordLatch, List<Boolean> checkedHashes, int startIndex) {
      this.startIndex = startIndex;
      this.checkedHashes = checkedHashes;
      this.checkPasswordLatch = checkPasswordLatch;
      this.transport = transport;
      this.handler = handler;
      this.passwords = passwords;
      this.hashes = hashes;
      this.socketInfo = socketInfo;
    }

    public void onComplete(List<Boolean> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.checkedHashes.set(this.startIndex + i, response.get(i));
      }
      this.transport.close();
      this.checkPasswordLatch.countDown();
    }

    public void onError(Exception e) {
      try {
        System.out.println("[checkPasswordCB onError, trying redistribution] " + e.getMessage());
        System.out.println("                                               @ " + this.socketInfo.toString());
        this.transport.close();
        BEsockets.remove(this.socketInfo);
        List<Boolean> response = this.handler.checkPassword(this.passwords, this.hashes);
        for (int i = 0; i < response.size(); ++i) {
          this.checkedHashes.set(this.startIndex + i, response.get(i));
        }
      } catch (Exception ex) {
        System.out.println("[checkPasswordCB - redistributing Exception] " + ex.getMessage());
      } finally {
        this.checkPasswordLatch.countDown();
      }
    }
  }
}

