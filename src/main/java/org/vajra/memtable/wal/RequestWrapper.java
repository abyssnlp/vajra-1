package org.vajra.memtable.wal;

import java.util.concurrent.CompletableFuture;

public class RequestWrapper<Req, Res> {

  private final CompletableFuture<Res> future;
  private final Req request;

  public RequestWrapper(Req request) {
    this.request = request;
    this.future = new CompletableFuture<>();
  }

  public CompletableFuture<Res> getFuture() {
    return future;
  }

  public Req getRequest() {
    return request;
  }

  public void complete(Res res) {
    future.complete(res);
  }

  public void completeWithException(Exception e) {
    e.printStackTrace();
    getFuture().completeExceptionally(e);
  }
}
