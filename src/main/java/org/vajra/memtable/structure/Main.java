package org.vajra.memtable.structure;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Hello world!");
    Node<Integer, String> node = new Node<>(42, "Shaurya");
    node.setValue("Chotu Noob");
    Node<?, ?> node2 = new Node<>();
    System.out.println(node);
    System.out.println(node2);

    RedBlackTree<Integer, String> rb = new RedBlackTree<>(node);
    Node<Integer, String> node3 = new Node<>(44, "Picard");
    Node<Integer, String> node4 = new Node<>(23, "Riker");
    Node<Integer, String> node5 = new Node<>(36, "Jordi");
    rb.insertNode(node3);
    rb.insertNode(node4);
    rb.insertNode(node5);
    System.out.println(rb);
    rb.deleteNode(23);
    System.out.println(rb);
    System.out.println(rb.searchNode(23));

    // Check concurrent access
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    System.out.println(executor.getPoolSize());
    System.out.println(executor.getActiveCount());

    //    Node<String, String> record1 = new Node<>("key", "123456");
    //    RedBlackTree<String, String> memtable = new RedBlackTree<>(record1);
    RedBlackTree<String, String> memtable = new RedBlackTree<>();
    // Nodes to put
    Node<String, String> record1 = new Node<>("key", "1234566");

    Node<String, String> record2 = new Node<>("secret", "abc12345");
    Node<String, String> record3 = new Node<>("sstables", "456789");
    Node<String, String> record4 = new Node<>("bumblebee", "autobots");
    Node<String, String> record5 = new Node<>("ssdsadad", "deceptfasdfvicon");
    Node<String, String> record6 = new Node<>("asdfssf", "asfdfeht");
    Node<String, String> record7 = new Node<>("dsf4g45g54", "wdef345");
    Node<String, String> record8 = new Node<>("fdfc45", "decept4rgercgicon");
    Node<String, String> record9 = new Node<>("esrgcesrg4", "ergct5h5");
    Node<String, String> record10 = new Node<>("ecg65", "ch56");
    Node<String, String> record11 = new Node<>("ecgerc4", "ecgg6");
    Node<String, String> record12 = new Node<>("cefrv556576", "sdf e234342");
    Node<String, String> record13 = new Node<>("sf ewfgg657", "fercgreth6");
    Node<String, String> record14 = new Node<>("wfcergy57", "wfxe2");

    List<Node<String, String>> toInsert = new ArrayList<>();
    toInsert.add(record1);
    toInsert.add(record2);
    toInsert.add(record3);
    toInsert.add(record4);
    toInsert.add(record5);
    toInsert.add(record6);
    toInsert.add(record7);
    toInsert.add(record8);
    toInsert.add(record9);
    toInsert.add(record10);
    toInsert.add(record11);
    toInsert.add(record12);
    toInsert.add(record13);
    toInsert.add(record14);

    CountDownLatch latch = new CountDownLatch(toInsert.size());
    toInsert.forEach(
        record ->
            executor.submit(
                () -> {
                  memtable.insertNode(record);
                  latch.countDown();
                }));
    latch.await();
    System.out.println(memtable);
    System.out.println(memtable.inorderTraversalWithKey());
    //    memtable.deleteNode("key");
    System.out.println(memtable.inorderTraversalWithKey());

    System.out.println(record14);
    System.out.println(memtable.searchNode("bumblebee"));
  }
}
