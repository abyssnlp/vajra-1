package org.vajra.memtable.structure;

@SuppressWarnings("ConstantValue")
public abstract class BaseBinaryTree<K, V> {

  abstract Node<K, V> getRoot();

  @Override
  public String toString() {
    return appendStringRecursive(getRoot(), new StringBuilder());
  }

  public String appendStringRecursive(Node<K, V> node, StringBuilder builder) {
    builder.append(node.getValue());
    if (node != null) {
      if (node.getLeft() != null) {
        builder.append("L{");
        appendStringRecursive(node.getLeft(), builder);
        builder.append("}");
      }
      if (node.getRight() != null) {
        builder.append("R{");
        appendStringRecursive(node.getRight(), builder);
        builder.append("}");
      }
    }
    return builder.toString();
  }
}
