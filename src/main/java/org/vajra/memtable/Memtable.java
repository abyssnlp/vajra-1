package org.vajra.memtable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.vajra.memtable.structure.Node;
import org.vajra.memtable.structure.RedBlackTree;
import org.vajra.memtable.wal.Request;
import org.vajra.memtable.wal.Response;
import org.vajra.memtable.wal.SingularUpdateQueue;
import org.vajra.memtable.wal.WALEntry;
import org.vajra.memtable.wal.WriteAheadLog;

public class Memtable<K extends Comparable<K>, V> {

  private static final String LOCATION = "/Users/shauryarawat/Documents/Databases/kv-store/wal";
  private final RedBlackTree<K, V> tree;
  private final SingularUpdateQueue<K, V> queue;
  private final WriteAheadLog<K, V> wal;

  public Memtable() throws IOException {
    this.tree = new RedBlackTree<>();
    wal = new WriteAheadLog<>(LOCATION);
    this.queue = new SingularUpdateQueue<>(wal);
    this.applyLog();
  }

  public void applyLog() {
    List<WALEntry> entries = this.wal.readAll();
    if (!entries.isEmpty()) {
      applyEntries(entries);
    }
  }

  @SuppressWarnings("unchecked")
  public void applyEntries(List<WALEntry> entries) {
    entries.forEach(
        entry -> {
          System.out.println("Applying entry: " + entry.toString());
          ByteArrayInputStream bis = new ByteArrayInputStream(entry.getKey());
          ByteArrayInputStream bisValue = new ByteArrayInputStream(entry.getValue());
          try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            ObjectInputStream oisValue = new ObjectInputStream(bisValue);
            K key = (K) ois.readObject();
            V value = (V) oisValue.readObject();
            String entryType = entry.getEntryType();
            switch (entryType) {
              case "SET":
                try {
                  tree.insertNode(new Node<>(key, value));
                } catch (IllegalArgumentException e) {
                  System.out.println("Key exists! Try updating instead (PUT)");
                }
                break;
              case "UPDATE":
                tree.updateNode(key, value);
                break;
              case "DELETE":
                tree.deleteNode(key);
                break;

              default:
                throw new IllegalStateException(
                    "EntryType not recognized by memtable: " + entryType);
            }
          } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(
                "Could not apply entry: " + entry.toString() + "Error: " + e);
          }
        });
  }

  public String getKey(K key) {
    Node<K, V> node = tree.searchNode(key);
    if (node == null) {
      return "Key" + key + " not found!";
    }
    return node.toString();
  }

  public void insertNode(K key, V value) {
    Node<K, V> node = new Node<>(key, value);
    // Write to WAL
    writeToWAL(key, value, "SET");
    // Finally, insert into node
    tree.insertNode(node);
  }

  public void insertNode(Node<K, V> node) {
    writeToWAL(node.getKey(), node.getValue(), "SET");
    tree.insertNode(node);
  }

  public void updateNode(K key, V value) {
    writeToWAL(key, value, "UPDATE");
    tree.updateNode(key, value);
  }

  public void updateNode(Node<K, V> node) {
    writeToWAL(node.getKey(), node.getValue(), "UPDATE");
    tree.updateNode(node.getKey(), node.getValue());
  }

  public void deleteNode(K key) {
    writeToWAL(key, null, "DELETE");
    tree.deleteNode(key);
  }

  public List<String> getAllNodes() {
    return tree.inorderTraversalWithKey();
  }

  private void writeToWAL(K key, V value, String entryType) {
    CompletableFuture<Response> response = queue.submit(new Request<>(entryType, key, value));
    response.whenComplete(
        (result, error) -> {
          System.out.println(result.toString());
          if (error != null) {
            error.printStackTrace();
            throw new RuntimeException(error);
          }
        });
  }
}
