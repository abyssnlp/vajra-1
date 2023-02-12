package org.vajra.kvstore;

import java.io.IOException;
import java.util.List;
import org.vajra.memtable.Memtable;
import org.springframework.stereotype.Service;

@Service
public class KVStoreService {

  private final Memtable<String, String> memtable = new Memtable<>();

  public KVStoreService() throws IOException {}

  public String getKey(String key) {
    return memtable.getKey(key);
  }

  public void insertNode(String key, String value) {
    memtable.insertNode(key, value);
  }

  public void updateNode(String key, String value) {
    memtable.updateNode(key, value);
  }

  public void deleteNode(String key) {
    memtable.deleteNode(key);
  }

  public List<String> getAllNodes() {
    return memtable.getAllNodes();
  }
}
