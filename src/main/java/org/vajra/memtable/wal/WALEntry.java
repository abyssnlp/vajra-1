package org.vajra.memtable.wal;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class WALEntry implements Serializable {

  private static final AtomicLong indexNum = new AtomicLong(1L);

  private final Long entryIndex;
  private final byte[] key;
  private final byte[] value;
  private final String entryType;
  private final Long timestamp;

  public WALEntry(byte[] key, byte[] value, String entryType) {
    this.entryIndex = indexNum.getAndIncrement();
    this.key = key;
    this.value = value;
    this.entryType = entryType;
    this.timestamp = Instant.now().toEpochMilli();
  }

  public WALEntry(Long entryIndex, byte[] key, byte[] value, String entryType, Long timestamp) {
    this.entryIndex = entryIndex;
    this.key = key;
    this.value = value;
    this.entryType = entryType;
    this.timestamp = timestamp;
  }

  public Long getEntryIndex() {
    return entryIndex;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public String getEntryType() {
    return entryType;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "WALEntry{"
        + "entryIndex="
        + entryIndex
        + ", key="
        + Arrays.toString(key)
        + ", value="
        + Arrays.toString(value)
        + ", entryType="
        + entryType
        + ", timestamp="
        + timestamp
        + '}';
  }
}
