package org.vajra.memtable.wal;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SingularUpdateQueue<K, V> {

  private final ArrayBlockingQueue<RequestWrapper<Request<K, V>, Response>> workQueue =
      new ArrayBlockingQueue<>(100);
  private final WriteAheadLog<K, V> wal;
  private volatile boolean isRunning = false;

  // TODO: Decouple function that the SUQ runs

  public SingularUpdateQueue(WriteAheadLog<K, V> wal) {
    this.wal = wal;
    // Worker Single thread executor to work off the queue
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(this::writeEntry);
  }

  public CompletableFuture<Response> submit(Request<K, V> req) {
    try {
      RequestWrapper<Request<K, V>, Response> wrapper = new RequestWrapper<>(req);
      workQueue.put(wrapper);
      return wrapper.getFuture();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<RequestWrapper<Request<K, V>, Response>> take() {
    try {
      return Optional.ofNullable(workQueue.poll(3, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      return Optional.empty();
    }
  }

  // Worker function to write to the WAL
  public void writeEntry() {
    isRunning = true;
    while (isRunning) {
      Optional<RequestWrapper<Request<K, V>, Response>> records = take();
      records.ifPresent(
          requestResponseRequestWrapper -> {
            System.out.println(
                "Writing to WAL " + requestResponseRequestWrapper.getRequest().toString());
            this.wal.writeEntry(requestResponseRequestWrapper.getRequest());
            requestResponseRequestWrapper.getFuture().complete(new Response("Done!"));
          });
    }
  }

  public void shutDown() {
    System.out.println("Shutting down!");
    this.isRunning = false;
  }
}
