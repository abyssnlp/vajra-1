package org.vajra.memtable.structure;

import java.util.ArrayList;
import java.util.List;

public abstract class BinarySearchTree<K, V> extends BaseBinaryTree<K, V> {

  abstract Node<K, V> searchNode(K key);

  abstract void insertNode(Node<K, V> node);

  abstract void deleteNode(K key);

  abstract void updateNode(K key, V value);

  public List<V> inorderTraversal() {
    List<V> elements = new ArrayList<>();
    Node<K, V> node = this.getRoot();
    getInorder(elements, node);
    return elements;
  }

  public List<String> inorderTraversalWithKey() {
    List<String> elements = new ArrayList<>();
    Node<K, V> node = this.getRoot();
    getInOrderWithKey(elements, node);
    return elements;
  }

  private void getInOrderWithKey(List<String> elements, Node<K, V> node) {
    if (node != null) {
      getInOrderWithKey(elements, node.getLeft());
      elements.add(node.getKey().toString() + "->" + node.getValue().toString());
      getInOrderWithKey(elements, node.getRight());
    }
  }

  public List<V> preorderTraversal() {
    List<V> elements = new ArrayList<>();
    Node<K, V> node = this.getRoot();
    getPreorder(elements, node);
    return elements;
  }

  public List<V> postorderTraversal() {
    List<V> elements = new ArrayList<>();
    Node<K, V> node = this.getRoot();
    getPostorder(elements, node);
    return elements;
  }

  private void getInorder(List<V> elems, Node<K, V> node) {
    if (node != null) {
      getInorder(elems, node.getLeft());
      elems.add(node.getValue());
      getInorder(elems, node.getRight());
    }
  }

  private void getPreorder(List<V> elems, Node<K, V> node) {
    if (node != null) {
      elems.add(node.getValue());
      getPreorder(elems, node.getLeft());
      getPreorder(elems, node.getRight());
    }
  }

  private void getPostorder(List<V> elems, Node<K, V> node) {
    if (node != null) {
      getPostorder(elems, node.getLeft());
      getPostorder(elems, node.getRight());
      elems.add(node.getValue());
    }
  }
}
