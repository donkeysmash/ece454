import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
  public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
    try {
      List<String> ret = new ArrayList<>(password.size());
      for (String x : password) {
        String hashedPwd = BCrypt.hashpw(x, BCrypt.gensalt(logRounds));
        ret.add(hashedPwd);
      }
      ret.add(oneHash);
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
        throw new Exception("password list size: " + pwdListSize ", hash list size: " + hashListSize);
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
}
