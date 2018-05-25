import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.TException;

public class Client {
  private static CountDownLatch hashPasswordLatch = new CountDownLatch(1);
  private static CountDownLatch checkPasswordLatch = new CountDownLatch(1);

  public static void main(String [] args) {
    if (args.length != 4) {
      System.err.println("Usage: java Client FE_host FE_port length logrounds");
      System.exit(-1);
    }

    try {
      TNonblockingTransport transport = new TNonblockingSocket(args[0], Integer.parseInt(args[1]));
      TProtocolFactory pf = new TBinaryProtocol.Factory();
      TAsyncClientManager clientManage = new TAsyncClientManager();
      BcryptService.AsyncClient client = new BcryptService.AsyncClient(pf, clientManage, transport);

      int lengthIwant = Integer.parseInt(args[2]);
      short logRoundsIwant = Short.parseShort(args[3]);

      List<String> passwords = new ArrayList<>(lengthIwant);
      for (int i = 0; i < lengthIwant; ++i) {
        passwords.add("sompaomdfspofm" + i);
      }
      passwords.add("");
      System.out.println("testing started         -- size: " + passwords.size());
      System.out.println("                        -- logR: " + logRoundsIwant);
      System.out.println();
      long startTime = System.nanoTime();
      List<String> hashed = new ArrayList<>(Collections.nCopies(passwords.size(), ""));
      client.hashPassword(passwords, logRoundsIwant, new HashPasswordCallback(hashed));
      hashPasswordLatch.await();
      long endTime = System.nanoTime();
      long duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
      System.out.println("hashPassword completed  -- size: " + hashed.size());
      System.out.println("                        -- time: " + duration + " ms");
      System.out.println();
      hashed.set(0, "somebadstringthatisntevenhash");
      startTime = System.nanoTime();
      List<Boolean> checked = new ArrayList<>(Collections.nCopies(passwords.size(), Boolean.FALSE));
      client.checkPassword(passwords, hashed, new CheckPasswordCallback(checked));
      checkPasswordLatch.await();
      endTime = System.nanoTime();
      duration = (endTime - startTime) / 1000000;  //divide by 1000000 to get milliseconds.
      System.out.println("chekcPassword completed -- size: " + hashed.size());
      System.out.println("                        -- time: " + duration + " ms");
      System.out.println();
      transport.close();
    } catch (Exception x) {
      x.printStackTrace();
    } 
  }

  static class HashPasswordCallback implements AsyncMethodCallback<List<String>> {
    private List<String> hashedList;

    public HashPasswordCallback(List<String> hashedList) {
      this.hashedList = hashedList;
    }

    public void onComplete(List<String> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.hashedList.set(i, response.get(i));
      }
      hashPasswordLatch.countDown();

    }

    public void onError(Exception e) {
      e.printStackTrace();
      hashPasswordLatch.countDown();
    }
  }

  static class CheckPasswordCallback implements AsyncMethodCallback<List<Boolean>> {
    private List<Boolean> checkList;

    public CheckPasswordCallback(List<Boolean> checkList) {
      this.checkList = checkList;
    }

    public void onComplete(List<Boolean> response) {
      for (int i = 0; i < response.size(); ++i) {
        this.checkList.set(i, response.get(i));
      }
      checkPasswordLatch.countDown();

    }

    public void onError(Exception e) {
      e.printStackTrace();
      checkPasswordLatch.countDown();
    }
  }

}

