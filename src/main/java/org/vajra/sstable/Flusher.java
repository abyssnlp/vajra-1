package org.vajra.sstable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.openjdk.jol.info.ClassLayout;
import org.vajra.memtable.structure.RedBlackTree;

public class Flusher<K extends Comparable<K>, V> implements Runnable {

  private final RedBlackTree<K, V> memTree;
  private final Long threshold;

  public Flusher(RedBlackTree<K, V> memTree) {
    this.memTree = memTree;
    this.threshold = (long) (10 * 1024 * 1024);
  }

  public Flusher(RedBlackTree<K, V> memTree, Long threshold) {
    this.memTree = memTree;
    this.threshold = threshold;
  }

  private long getTreeSize() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(this.memTree);
    return baos.toByteArray().length;
  }

  @Override
  public void run() {
    System.out.println(
        "Memtable size: "
            + ClassLayout.parseInstance(this.memTree).instanceSize()
            + " bytes, Threshold: "
            + this.threshold);
    try {
      System.out.println("Size from baos: " + getTreeSize());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
