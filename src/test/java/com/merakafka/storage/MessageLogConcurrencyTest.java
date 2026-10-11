
package com.merakafka.storage;

import com.merakafka.Message;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MessageLogConcurrencyTest {

    private static final int THREAD_COUNT = 8;
    private static final int MESSAGES_PER_THREAD = 100;

    public static void main(String[] args) throws Exception {
        System.setProperty("merakafka.segment.max.bytes", "1000");

        Path tempDir = Files.createTempDirectory("concurrency-test-");

        try {
            MessageLog log = new MessageLog(
                tempDir.resolve("message.log")
            );

            ExecutorService executor =
                Executors.newFixedThreadPool(THREAD_COUNT);

            CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
            CountDownLatch start = new CountDownLatch(1);

            try {
                @SuppressWarnings("unchecked")
                Future<?>[] futures = new Future<?>[THREAD_COUNT];

                for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
                    final int id = threadId;

                    futures[threadId] = executor.submit(() -> {
                        ready.countDown();
                        start.await();

                        for (int i = 0; i < MESSAGES_PER_THREAD; i++) {
                            log.append(
                                new Message(
                                    "concurrency-topic",
                                    "thread-" + id + "-message-" + i
                                )
                            );
                        }

                        return null;
                    });
                }

                ready.await();
                start.countDown();

                for (Future<?> future : futures) {
                    future.get();
                }

            } finally {
                executor.shutdownNow();
            }

            int expectedCount = THREAD_COUNT * MESSAGES_PER_THREAD;

            if (log.size() != expectedCount) {
                throw new AssertionError(
                    "Expected " + expectedCount
                        + " messages, got " + log.size()
                );
            }

            Set<Long> offsets = new HashSet<>();

            for (int i = 0; i < expectedCount; i++) {
                Message message = log.read(i);

                if (message == null) {
                    throw new AssertionError(
                        "Missing message at offset " + i
                    );
                }

                if (message.getOffset() != i) {
                    throw new AssertionError(
                        "Expected offset " + i
                            + ", got " + message.getOffset()
                    );
                }

                if (!offsets.add(message.getOffset())) {
                    throw new AssertionError(
                        "Duplicate offset " + message.getOffset()
                    );
                }

                if (!message.getValue().startsWith("thread-")) {
                    throw new AssertionError(
                        "Unexpected message value at offset " + i
                    );
                }
            }

            if (offsets.size() != expectedCount) {
                throw new AssertionError("Offsets are not unique");
            }

            // Verify the persisted log can be recovered.
            MessageLog recovered = new MessageLog(
                tempDir.resolve("message.log")
            );

            if (recovered.size() != expectedCount) {
                throw new AssertionError(
                    "Recovery expected " + expectedCount
                        + " messages, got " + recovered.size()
                );
            }

            List<Message> batch =
                recovered.readFrom(0, Integer.MAX_VALUE);

            if (batch.size() != expectedCount) {
                throw new AssertionError(
                    "Expected " + expectedCount
                        + " recovered messages in range read, got "
                        + batch.size()
                );
            }

            System.out.println(
                "PASS: concurrent appends produce unique continuous offsets"
            );
            System.out.println(
                "PASS: recovery after concurrent appends"
            );
            System.out.println(
                "Messages written: " + expectedCount
            );

        } finally {
            try (var files = Files.walk(tempDir)) {
                files.sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
        }
    }
}
