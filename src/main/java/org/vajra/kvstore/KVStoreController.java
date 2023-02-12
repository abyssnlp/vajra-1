package org.vajra.kvstore;

import java.util.List;
import org.vajra.memtable.structure.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "api/v1/store")
public class KVStoreController {

  private final KVStoreService kvStoreService;

  @Autowired
  public KVStoreController(KVStoreService kvStoreService) {
    this.kvStoreService = kvStoreService;
  }

  @GetMapping(path = "{key}")
  public String searchKey(@PathVariable("key") String key) {
    return kvStoreService.getKey(key.trim());
  }

  @PostMapping
  public void insertNode(@RequestBody Node<String, String> node) {
    kvStoreService.insertNode(node.getKey(), node.getValue());
  }

  @PutMapping
  public void updateNode(@RequestBody Node<String, String> node) {
    kvStoreService.updateNode(node.getKey(), node.getValue());
  }

  @DeleteMapping(path = "{key}")
  public void deleteNode(@PathVariable("key") String key) {
    kvStoreService.deleteNode(key);
  }

  @GetMapping
  public List<String> getAllNodes() {
    return kvStoreService.getAllNodes();
  }
}
