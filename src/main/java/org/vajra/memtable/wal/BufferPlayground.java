package org.vajra.memtable.wal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BufferPlayground {

  public static void main(String[] args) throws IOException {

    String playgroundLocation = "/Users/shauryarawat/Documents/Databases/kv-store/play";
    FileOutputStream fos = new FileOutputStream(playgroundLocation);
    DataOutputStream dos = new DataOutputStream(fos);

    // Example format: int length of entryType, entryType
    String command = EntryType.SET.command;
    System.out.println(command);
    System.out.println(command.getBytes().length);
    dos.writeInt(command.getBytes().length);
    dos.writeBytes(command);
    dos.flush();

    String command2 = EntryType.UPDATE.command;
    System.out.println(command2);
    System.out.println(command2.getBytes().length);
    dos.writeInt(command2.getBytes().length);
    dos.writeBytes(command2);
    dos.flush();

    // Try Reading them from the file
    FileInputStream fis = new FileInputStream(playgroundLocation);
    DataInputStream dis = new DataInputStream(fis);

    System.out.println(fis.available());

    int testInt1 = dis.readInt();
    System.out.println(testInt1);
    byte[] readBuffer = new byte[testInt1];
    dis.read(readBuffer);
    int testInt2 = dis.readInt();
    System.out.println(testInt2);
    byte[] readBuffer2 = new byte[testInt2];
    dis.read(readBuffer2);
    System.out.println(new String(readBuffer, StandardCharsets.UTF_8));
    System.out.println(new String(readBuffer2, StandardCharsets.UTF_8));

    fos.close();
  }
}
