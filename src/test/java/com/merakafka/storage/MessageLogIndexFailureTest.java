
package com.merakafka.storage;

import com.merakafka.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MessageLogIndexFailureTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("merakafka.segment.max.bytes", "100000");

        Path directory = Files.createTempDirectory("merakafka-index-failure-");
        Path logFile = directory.resolve("message.log");

        MessageLog log = new MessageLog(logFile);

        // Offset 0 creates the first sparse-index entry.
        long offset0 = log.append(new Message("demo", "message-0"));

        if (offset0 != 0) {
            throw new AssertionError("Expected offset 0, got " + offset0);
        }

        // Make the index unusable so the next index write fails.
        Path indexFile = directory.resolve(
                "00000000000000000000.index");

        Files.delete(indexFile);
        Files.createDirectory(indexFile);

        // Offsets 1-9 do not require sparse-index writes.
        for (int i = 1; i < 10; i++) {
            long offset = log.append(new Message("demo", "message-" + i));

            if (offset != i) {
                throw new AssertionError(
                        "Expected offset " + i + ", got " + offset);
            }
        }

        // Offset 10 triggers an index write, which should fail safely.
        long offset10 = log.append(new Message("demo", "message-10"));

        if (offset10 != 10) {
            throw new AssertionError(
                    "Expected offset 10, got " + offset10);
        }

        if (log.size() != 11) {
            throw new AssertionError(
                    "Expected size 11, got " + log.size());
        }

        Message fetched = log.read(10);

        if (fetched == null
                || fetched.getOffset() != 10
                || !"message-10".equals(fetched.getValue())) {
            throw new AssertionError(
                    "Message at offset 10 was not read correctly");
        }

        System.out.println("PASS: index failure does not reuse offsets");
        System.out.println("PASS: message remains readable after index failure");
        System.out.println("PASS: log size remains correct");
    }
}
