package org.vajra.memtable.wal;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog<K, V> {

  private final DataOutputStream dos;
  private final String location;

  public WriteAheadLog(String location) throws IOException {
    this.location = location;
    FileOutputStream fos = new FileOutputStream(location, true);
    this.dos = new DataOutputStream(fos);
  }

  public void writeEntry(Request<K, V> req) {
    ByteArrayOutputStream bos;
    ObjectOutputStream oos;
    ByteArrayOutputStream bosValue;
    ObjectOutputStream oosValue;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(req.getKey());
      oos.flush();
      byte[] key = bos.toByteArray();

      bosValue = new ByteArrayOutputStream();
      oosValue = new ObjectOutputStream(bosValue);
      oosValue.writeObject(req.getValue());
      oosValue.flush();
      byte[] value = bosValue.toByteArray();

      WALEntry entry = new WALEntry(key, value, req.getEntryType());

      Long entryIndex = entry.getEntryIndex();
      int keySize = key.length;
      int valueSize = value.length;
      int entrySize = req.getEntryType().getBytes().length;
      byte[] entryTypeBuf = req.getEntryType().getBytes();
      Long timestamp = entry.getTimestamp();

      dos.writeLong(entryIndex);
      dos.writeInt(keySize);
      dos.write(key);
      dos.writeInt(valueSize);
      dos.write(value);
      dos.writeInt(entrySize);
      dos.write(entryTypeBuf);
      dos.writeLong(timestamp);

      dos.flush();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public List<WALEntry> readAll() {
    List<WALEntry> entries = new ArrayList<>();
    FileInputStream fis;
    try {
      fis = new FileInputStream(location);
      DataInputStream dis = new DataInputStream(fis);
      boolean run = true;
      while (run) {
        int obj = fis.available();
        if (obj <= 0) {
          run = false;
        } else {
          Long entryIndex = dis.readLong();
          int keySize = dis.readInt();
          byte[] keyBuf = new byte[keySize];
          int keyBufBytes = dis.read(keyBuf);
          if (keyBufBytes <= 0) {
            throw new RuntimeException("Could not parse key from buffer!");
          }
          //          K key = (K) keyBuf;
          int valueSize = dis.readInt();
          byte[] valueBuf = new byte[valueSize];
          int valueBufBytes = dis.read(valueBuf);
          if (valueBufBytes <= 0) {
            throw new RuntimeException("Could not parse value from buffer!");
          }
          //          V value = (V) valueBuf;
          int entrySize = dis.readInt();
          byte[] entryBuf = new byte[entrySize];
          int entryBufBytes = dis.read(entryBuf);
          if (entryBufBytes <= 0) {
            throw new RuntimeException("Could not parse entryType from buffer!");
          }
          String entryType = new String(entryBuf, StandardCharsets.UTF_8);
          Long timestamp = dis.readLong();
          WALEntry entry = new WALEntry(entryIndex, keyBuf, valueBuf, entryType, timestamp);
          entries.add(entry);
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return entries;
  }
}
