package org.vajra.memtable.wal;

public class Request<K, V> {

  private String entryType;
  private K key;
  private V value;

  public Request(String entryType, K key, V value) {
    this.entryType = entryType;
    this.key = key;
    this.value = value;
  }

  public String getEntryType() {
    return entryType;
  }

  public void setEntryType(String entryType) {
    this.entryType = entryType;
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

  @Override
  public String toString() {
    return "WALRequest{"
        + "entryType='"
        + entryType
        + '\''
        + ", key="
        + key
        + ", value="
        + value
        + '}';
  }
}
