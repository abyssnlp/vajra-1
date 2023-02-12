package org.vajra.memtable.structure;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedBlackTree<K extends Comparable<K>, V> extends BinarySearchTree<K, V> {

  // Reentrant RW Lock
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock writeLock = rwLock.writeLock();
  private final Lock readLock = rwLock.readLock();

  private Node<K, V> root;

  public RedBlackTree() {}

  public RedBlackTree(Node<K, V> node) {
    this.root = node;
  }

  @Override
  public Node<K, V> getRoot() {
    return root;
  }

  /**
   * Search for a node in the Tree
   *
   * @param key Key of the KV pair
   * @return Node with the specified key
   */
  @Override
  public Node<K, V> searchNode(K key) {
    readLock.lock();
    try {
      Node<K, V> node = root;
      while (node != null) {
        if (key.compareTo(node.getKey()) == 0) {
          return node;
        } else if (key.compareTo(node.getKey()) < 0) {
          node = node.getLeft();
        } else {
          node = node.getRight();
        }
      }
    } finally {
      readLock.unlock();
    }
    return null;
  }

  /**
   * Insert a node into the RB Tree. Fixes RB properties post-insert. Search for insertion position
   * from root and attach to leaf or half-leaf as a red leaf.
   *
   * @param node Node to be inserted
   */
  @Override
  public void insertNode(Node<K, V> node) {
    writeLock.lock();
    try {
      Node<K, V> n = root;
      Node<K, V> parent = null;
      while (n != null) {
        parent = n;
        if (node.getKey().compareTo(n.getKey()) < 0) {
          n = n.getLeft();
        } else if (node.getKey().compareTo(n.getKey()) > 0) {
          n = n.getRight();
        } else {
          throw new IllegalArgumentException("Key already exists");
        }
      }
      // if parent is null then the inserted node is root
      if (parent == null) {
        this.root = node;
      } else if (node.getKey().compareTo(parent.getKey()) < 0) {
        parent.setLeft(node);
      } else {
        parent.setRight(node);
      }
      node.setParent(parent);
      fixRedBlackPropertiesPostInsert(node);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void updateNode(K key, V value) {
    writeLock.lock();
    try {
      Node<K, V> foundNode = searchNode(key);
      if (foundNode != null) {
        foundNode.setValue(value);
      } else {
        insertNode(new Node<>(key, value));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteNode(K key) {
    writeLock.lock();
    try {
      // Find node to delete
      Node<K, V> node = getRoot();
      while (node != null && key.compareTo(node.getKey()) != 0) {
        if (key.compareTo(node.getKey()) < 0) {
          node = node.getLeft();
        } else {
          node = node.getRight();
        }
      }
      if (node == null) {
        return;
      }
      // If node is not null, we have the node to be deleted
      // 2 nodes are required to repair post-delete
      Node<K, V> movedUpNode;
      Color deletedNodeColor;

      // Case 1: Node has 0 or 1 child
      if (node.getLeft() == null || node.getRight() == null) {
        movedUpNode = deleteNodeWithZeroOrOneChild(node);
        deletedNodeColor = node.getColor();
      } else {
        // Node has 2 children, the content of the inorder successor
        // is copied and the successor is deleted
        Node<K, V> inorderSuccessor = getInorderSuccessor(node);
        node.setKey(inorderSuccessor.getKey());
        node.setValue(inorderSuccessor.getValue());
        movedUpNode = deleteNodeWithZeroOrOneChild(inorderSuccessor);
        deletedNodeColor = inorderSuccessor.getColor();
      }

      if (deletedNodeColor == Color.BLACK) {
        fixRedBlackPropertiesPostDelete(movedUpNode);
        if (movedUpNode.getKey() == null && movedUpNode.getValue() == null) {
          replaceParentsChild(movedUpNode.getParent(), movedUpNode, null);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void fixRedBlackPropertiesPostDelete(Node<K, V> node) {
    // Case 1: Node is root
    if (node == getRoot()) {
      return;
    }
    Node<K, V> sibling = getSibling(node);
    // Case 2: Sibling is red
    if (sibling.getColor() == Color.RED) {
      handleRedSibling(node, sibling);
      sibling = getSibling(node); // new sibling post fix(rotation)
    }

    // Case 3 + 4: Black sibling with two black children
    if (isBlack(sibling.getLeft()) && isBlack(sibling.getRight())) {
      sibling.setColor(Color.RED);
      // Case 3: Black sibling with 2 black children and red parent
      if (node.getParent().getColor() == Color.RED) {
        node.getParent().setColor(Color.BLACK);
      } else {
        // Case 4: Black sibling with 2 black children and black parent
        fixRedBlackPropertiesPostDelete(node.getParent());
      }
    } else {
      // Case 5 + 6: Black sibling with atleast 1 red child
      handleBlackSiblingWithAtLeastOneRedChild(node, sibling);
    }
  }

  private void handleBlackSiblingWithAtLeastOneRedChild(Node<K, V> node, Node<K, V> sibling) {
    boolean nodeIsLeft = node.getParent().getLeft() == node;
    // Case 5: Black sibling, atleast 1 red child, outer nephew black
    // Recolor sibling and its child(nephew) and rotate around sibling
    if (nodeIsLeft && isBlack(sibling.getRight())) {
      sibling.getLeft().setColor(Color.BLACK);
      sibling.setColor(Color.RED);
      rotateRight(sibling);
      sibling = node.getParent().getRight();
    } else if (!nodeIsLeft && isBlack(sibling.getLeft())) {
      sibling.getRight().setColor(Color.BLACK);
      sibling.setColor(Color.RED);
      rotateLeft(sibling);
      sibling = node.getParent().getLeft();
    }
    // Fall through to Case 6
    // Case 6: Black sibling, atleast 1 red child, outer nephew is RED
    // Recolor sibling + parent + nephew + rotate around parent
    sibling.setColor(node.getParent().getColor());
    node.getParent().setColor(Color.BLACK);
    if (nodeIsLeft) {
      sibling.getRight().setColor(Color.BLACK);
      rotateLeft(node.getParent());
    } else {
      sibling.getLeft().setColor(Color.BLACK);
      rotateRight(node.getParent());
    }
  }

  private boolean isBlack(Node<K, V> node) {
    return node == null || node.getColor() == Color.BLACK;
  }

  private void handleRedSibling(Node<K, V> node, Node<K, V> sibling) {
    node.getParent().setColor(Color.RED);
    sibling.setColor(Color.BLACK);

    if (node == node.getParent().getLeft()) {
      rotateLeft(node.getParent());
    } else {
      rotateRight(node.getParent());
    }
  }

  private Node<K, V> getSibling(Node<K, V> node) {
    Node<K, V> parent = node.getParent();
    if (node == parent.getLeft()) {
      return parent.getRight();
    } else if (node == parent.getRight()) {
      return parent.getLeft();
    } else {
      throw new IllegalStateException("Parent: " + parent + " does not have child: " + node);
    }
  }

  private Node<K, V> getInorderSuccessor(Node<K, V> node) {
    Node<K, V> successor = node.getRight();
    while (successor.getLeft() != null) {
      successor = successor.getLeft();
    }
    return successor;
  }

  private Node<K, V> deleteNodeWithZeroOrOneChild(Node<K, V> node) {
    if (node.getLeft() != null) {
      replaceParentsChild(node.getParent(), node, node.getLeft());
      return node.getLeft();
    } else if (node.getRight() != null) {
      replaceParentsChild(node.getParent(), node, node.getRight());
      return node.getRight();
    } else {
      Node<K, V> newChild;
      if (node.getColor() == Color.BLACK) {
        newChild = new Node<>();
      } else {
        newChild = null;
      }
      replaceParentsChild(node.getParent(), node, newChild);
      return newChild;
    }
  }

  private void fixRedBlackPropertiesPostInsert(Node<K, V> node) {
    Node<K, V> parent = node.getParent();

    // Case 1: Inserted Node is root
    if (parent == null) {
      node.setColor(Color.BLACK);
      return;
    }
    if (parent.getColor() == Color.BLACK) {
      return;
    }
    // From here on, parent is RED
    Node<K, V> grandParent = parent.getParent();

    // Case 2: If grandparent is NULL, then the parent is root
    if (grandParent == null) {
      parent.setColor(Color.BLACK);
      return;
    }
    // Get uncle
    Node<K, V> uncle = getUncle(parent);

    // Case 3: If both parent and uncle are RED
    if (uncle != null && uncle.getColor() == Color.RED) {
      grandParent.setColor(Color.RED);
      uncle.setColor(Color.BLACK);
      parent.setColor(Color.BLACK);
      // fix grandparent recursively
      fixRedBlackPropertiesPostInsert(grandParent);
    }
    // parent is the left child of the grandparent
    else if (parent == grandParent.getLeft()) {
      // Case 4a: Uncle if black, parent is red and node is parent's inner child (right)
      // if parent is left, then inner node is the right child
      if (node == parent.getRight()) {
        rotateLeft(parent);
        parent = node;
      }
      rotateRight(grandParent);
      parent.setColor(Color.BLACK);
      grandParent.setColor(Color.RED);
    }
    // Parent is right child of the grandParent
    // if parent - right, inner child is the left node
    else {
      if (node == parent.getLeft()) {
        rotateRight(parent);
        parent = node;
      }
      rotateLeft(grandParent);
      parent.setColor(Color.BLACK);
      grandParent.setColor(Color.RED);
    }
  }

  private Node<K, V> getUncle(Node<K, V> parent) {
    Node<K, V> grandParent = parent.getParent();
    if (grandParent.getLeft() == parent) {
      return grandParent.getRight();
    } else if (grandParent.getRight() == parent) {
      return grandParent.getLeft();
    } else {
      throw new IllegalStateException("Parent is not a child of the grandParent node!");
    }
  }

  private void rotateRight(Node<K, V> node) {
    Node<K, V> parent = node.getParent();
    Node<K, V> leftChild = node.getLeft();

    node.setLeft(leftChild.getRight());
    if (leftChild.getRight() != null) {
      leftChild.getRight().setParent(node);
    }
    leftChild.setRight(node);
    node.setParent(leftChild);
    replaceParentsChild(parent, node, leftChild);
  }

  private void rotateLeft(Node<K, V> node) {
    Node<K, V> parent = node.getParent();
    Node<K, V> rightChild = node.getRight();

    node.setRight(rightChild.getLeft());
    if (rightChild.getLeft() != null) {
      rightChild.getLeft().setParent(node);
    }
    rightChild.setLeft(node);
    node.setParent(rightChild);
    replaceParentsChild(parent, node, rightChild);
  }

  private void replaceParentsChild(Node<K, V> parent, Node<K, V> oldChild, Node<K, V> newChild) {
    if (parent == null) {
      this.root = newChild;
    } else if (parent.getRight() == oldChild) {
      parent.setRight(newChild);
    } else if (parent.getLeft() == oldChild) {
      parent.setLeft(newChild);
    } else {
      throw new IllegalArgumentException("Node is not a child of the parent node!");
    }
    if (newChild != null) {
      newChild.setParent(parent);
    }
  }
}
