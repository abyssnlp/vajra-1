package org.vajra.memtable.wal;

public class Response {

  private final String status;

  public Response(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "WALResponse{" + "status='" + status + '\'' + '}';
  }
}
