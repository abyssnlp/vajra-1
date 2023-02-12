package org.vajra.memtable.wal;

public enum EntryType {
  SET("SET"),
  DELETE("DELETE"),
  UPDATE("UPDATE");

  public final String command;

  EntryType(String command) {
    this.command = command;
  }
}
