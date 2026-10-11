
package com.merakafka.storage;

import com.merakafka.Message;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MessageLogRecoveryTest {

    public static void main(String[] args) throws Exception {
        Path tempDir = Files.createTempDirectory("recovery-test-");

        try {
            Path logFile = tempDir.resolve("message.log");
            MessageLog log = new MessageLog(logFile);

            log.append(new Message("recovery-topic", "valid-0"));
            log.append(new Message("recovery-topic", "valid-1"));

            Path segment;

            try (var paths = Files.list(tempDir)) {
                segment = paths
                        .filter(path -> path.getFileName().toString()
                                .matches("\\d{20}\\.log"))
                        .findFirst()
                        .orElseThrow();
            }

            long validSize = Files.size(segment);

            // Simulate a crash leaving an incomplete final record.
            Files.write(
                    segment,
                    "2|partial-record".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND
            );

            MessageLog recovered = new MessageLog(logFile);

            if (recovered.size() != 2) {
                throw new AssertionError(
                        "Expected 2 recovered messages, got " + recovered.size()
                );
            }

            if (!"valid-0".equals(recovered.read(0).getValue())) {
                throw new AssertionError("Message at offset 0 was not recovered");
            }

            if (!"valid-1".equals(recovered.read(1).getValue())) {
                throw new AssertionError("Message at offset 1 was not recovered");
            }

            if (Files.size(segment) != validSize) {
                throw new AssertionError(
                        "Incomplete trailing record was not truncated"
                );
            }

            long newOffset = recovered.append(
                    new Message("recovery-topic", "valid-2")
            );

            if (newOffset != 2) {
                throw new AssertionError(
                        "Expected next offset 2, got " + newOffset
                );
            }

            System.out.println("PASS: valid records survive recovery");
            System.out.println("PASS: incomplete trailing record is truncated");
            System.out.println("PASS: appending continues at the correct offset");

        } finally {
            try (var paths = Files.walk(tempDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
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
