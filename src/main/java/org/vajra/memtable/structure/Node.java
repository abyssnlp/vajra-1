package org.vajra.memtable.structure;

public class Node<K, V> {

  private K key;
  private V value;
  private Node<K, V> left;
  private Node<K, V> right;
  private Node<K, V> parent;
  private Color color;

  public Node() {
    this.color = Color.BLACK;
  }

  public Node(K key, V value) {
    this.key = key;
    this.value = value;
    this.color = Color.RED;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  public Node<K, V> getLeft() {
    return left;
  }

  public void setLeft(Node<K, V> left) {
    this.left = left;
  }

  public Node<K, V> getRight() {
    return right;
  }

  public void setRight(Node<K, V> right) {
    this.right = right;
  }

  public Node<K, V> getParent() {
    return parent;
  }

  public void setParent(Node<K, V> parent) {
    this.parent = parent;
  }

  public Color getColor() {
    return color;
  }

  public void setColor(Color color) {
    this.color = color;
  }

  public String toString() {
    return "Node{"
        + "key="
        + this.getKey()
        + ", value="
        + this.getValue()
        + ", color="
        + this.getColor()
        + '}';
  }
}
