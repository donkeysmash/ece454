service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void put2(1: string key, 2: string value);

  void putReplica(1: string key, 2: string value);

  void copySnapshot();
  map<string, string> getSnapshot();
}
