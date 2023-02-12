package org.vajra.memtable.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WALTry {

  /** */
  public static void main(String[] args) throws InterruptedException, IOException {
    Request<Integer, String> request = new Request<>("SET", 42, "jacob");
    System.out.println(request);
    WriteAheadLog<Integer, String> wal =
        new WriteAheadLog<>("/Users/shauryarawat/Documents/Databases/kv-store/wal");
    SingularUpdateQueue<Integer, String> queue = new SingularUpdateQueue<>(wal);
    CompletableFuture<Response> response1 = queue.submit(request);
    Request<Integer, String> request2 = new Request<>("UPDATE", 55, "lmao");
    Request<Integer, String> request3 = new Request<>("DELETE", 55, null);
    Thread.sleep(1000);
    CompletableFuture<Response> response2 = queue.submit(request2);
    Thread.sleep(1000);
    CompletableFuture<Response> response3 = queue.submit(request3);
    //    List.of(response1, response2, response3)
    //        .forEach(res -> res.whenComplete((response, error) -> {
    //          System.out.println("Returned " + response.toString());
    //          if (error != null) {
    //            error.printStackTrace();
    //          }
    //        }));
    List<?> results =
        Stream.of(response1, response2, response3)
            .map(
                res ->
                    res.whenComplete(
                        (response, error) -> {
                          System.out.println("Returned " + response.toString());
                        }))
            .map(CompletableFuture::toString)
            .collect(Collectors.toList());
    System.out.println(results);

    System.out.println(wal.readAll());

    Runtime.getRuntime().addShutdownHook(new Thread(queue::shutDown));
  }
}
