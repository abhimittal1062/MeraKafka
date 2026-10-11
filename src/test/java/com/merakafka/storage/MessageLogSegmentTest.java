
package com.merakafka.storage;

import com.merakafka.Message;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;

public class MessageLogSegmentTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("merakafka.segment.max.bytes", "100");

        Path tempDir = Files.createTempDirectory("segment-test-");

        try {
            Path legacyLog = tempDir.resolve("message.log");
            MessageLog log = new MessageLog(legacyLog);

            // 1. Append messages and verify offsets.
            for (int i = 0; i < 10; i++) {
                long offset = log.append(
                    new Message("test-topic", "message-" + i)
                );

                if (offset != i) {
                    throw new AssertionError(
                        "Expected offset " + i + ", got " + offset
                    );
                }
            }

            // 2. Read all messages across segments.
            List<Message> messages =
                log.readFrom(0, Integer.MAX_VALUE);

            if (messages.size() != 10) {
                throw new AssertionError(
                    "Expected 10 messages, got " + messages.size()
                );
            }

            for (int i = 0; i < 10; i++) {
                assertMessage(messages.get(i), i);
            }

            // 3. Verify segment rotation.
            long segmentCount;

            try (var files = Files.list(tempDir)) {
                segmentCount = files
                    .filter(path -> path.getFileName().toString().endsWith(".log"))
                    .filter(path -> !path.getFileName().toString()
                        .equals("message.log"))
                    .count();
            }

            if (segmentCount < 2) {
                throw new AssertionError(
                    "Expected multiple segments, got " + segmentCount
                );
            }

            System.out.println(
                "PASS: segment rotation and cross-segment reads"
            );
            System.out.println("Segments created: " + segmentCount);

            // 4. Reopen the log and verify recovery.
            MessageLog recovered = new MessageLog(legacyLog);

            if (recovered.size() != 10) {
                throw new AssertionError(
                    "Expected 10 recovered messages, got "
                        + recovered.size()
                );
            }

            List<Message> recoveredMessages =
                recovered.readFrom(0, Integer.MAX_VALUE);

            if (recoveredMessages.size() != 10) {
                throw new AssertionError(
                    "Not all messages recovered"
                );
            }

            for (int i = 0; i < 10; i++) {
                assertMessage(recoveredMessages.get(i), i);
            }

            System.out.println("PASS: recovery after segment rotation");

            // 5. Verify the next offset after recovery.
            long nextOffset = recovered.append(
                new Message("test-topic", "message-10")
            );

            if (nextOffset != 10) {
                throw new AssertionError(
                    "Expected next offset 10, got " + nextOffset
                );
            }

            // 6. Verify invalid offsets and byte limits.
            if (!recovered.readFrom(0, 1).isEmpty()) {
                throw new AssertionError(
                    "Expected empty result when first message exceeds limit"
                );
            }

            if (!recovered.readFrom(0, 0).isEmpty()) {
                throw new AssertionError(
                    "Zero byte limit should return empty"
                );
            }

            if (!recovered.readFrom(-1, 1000).isEmpty()) {
                throw new AssertionError(
                    "Negative offset should return empty"
                );
            }

            if (!recovered.readFrom(1000, 1000).isEmpty()) {
                throw new AssertionError(
                    "Out-of-range offset should return empty"
                );
            }

            System.out.println("PASS: byte-limit edge cases");

            // 7. Verify exact byte-limit behaviour.
            Message firstMessage = recovered.read(0);
            Message secondMessage = recovered.read(1);

            int firstMessageBytes =
                serializedRecordBytes(firstMessage);

            int secondMessageBytes =
                serializedRecordBytes(secondMessage);

            List<Message> exactFit =
                recovered.readFrom(0, firstMessageBytes);

            if (exactFit.size() != 1
                    || exactFit.get(0).getOffset() != 0) {
                throw new AssertionError(
                    "Expected exactly one message at the first-message limit"
                );
            }

            List<Message> twoFit = recovered.readFrom(
                0, firstMessageBytes + secondMessageBytes
            );

            if (twoFit.size() != 2) {
                throw new AssertionError(
                    "Expected exactly two messages, got " + twoFit.size()
                );
            }

            System.out.println("PASS: exact byte-limit behaviour");

            // 8. Append more messages to exercise sparse-index boundaries.
            for (int i = 11; i < 31; i++) {
                long offset = recovered.append(
                    new Message("test-topic", "message-" + i)
                );

                if (offset != i) {
                    throw new AssertionError(
                        "Expected offset " + i + ", got " + offset
                    );
                }
            }

            // 9. Reopen again to force index rebuilding from disk.
            MessageLog indexedRecovery = new MessageLog(legacyLog);

            if (indexedRecovery.size() != 31) {
                throw new AssertionError(
                    "Expected 31 recovered messages, got "
                        + indexedRecovery.size()
                );
            }

            // Check offsets immediately before, at, and after index intervals.
            int[] offsetsToCheck = {
                0, 9, 10, 11, 19, 20, 21, 29, 30
            };

            for (int offset : offsetsToCheck) {
                Message message = indexedRecovery.read(offset);
                assertMessage(message, offset);

                List<Message> batch = indexedRecovery.readFrom(
                    offset, Integer.MAX_VALUE
                );

                if (batch.isEmpty()
                        || batch.get(0).getOffset() != offset) {
                    throw new AssertionError(
                        "Range read failed at offset " + offset
                    );
                }
            }

            System.out.println(
                "PASS: sparse-index reads and recovery"
            );

        } finally {
            // 10. Clean up the temporary test directory.
            try (var files = Files.walk(tempDir)) {
                files.sorted(Comparator.reverseOrder())
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

    private static void assertMessage(Message message, int expectedOffset) {
        if (message == null
                || message.getOffset() != expectedOffset
                || !message.getValue().equals(
                    "message-" + expectedOffset
                )) {
            throw new AssertionError(
                "Message mismatch at offset " + expectedOffset
            );
        }
    }

    private static int serializedRecordBytes(Message message) {
        String topic = Base64.getEncoder().encodeToString(
            message.getTopic().getBytes(StandardCharsets.UTF_8)
        );

        String value = Base64.getEncoder().encodeToString(
            message.getValue().getBytes(StandardCharsets.UTF_8)
        );

        String record = message.getOffset()
            + "|" + topic
            + "|" + value
            + "|" + message.getTimestamp().toEpochMilli()
            + "\n";

        return record.getBytes(StandardCharsets.UTF_8).length;
    }
}
