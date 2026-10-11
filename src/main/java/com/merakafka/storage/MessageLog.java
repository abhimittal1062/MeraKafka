package com.merakafka.storage;

import com.merakafka.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Base64;
import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;

public class MessageLog {

    private static final int INDEX_INTERVAL=10;
    
    // Default segment size: 1 MB.
    // Can be overridden using -Dmerakafka.segment.max.bytes=...
    private static final long MAX_SEGMENT_BYTES=Long.getLong("merakafka.segment.max.bytes",1024*1024);
    private static final Pattern SEGMENT_PATTERN=Pattern.compile("\\d{20}\\.log");
    // private final List<Message> messages = new ArrayList<>();
    private final Path directory;
    private final Path legacyLogFile;

    private Path activeSegment;
    private long activeSegmentSize;
    private long nextOffset;
    private final Set<Path> unusableIndexes = new HashSet<>();

    public MessageLog(Path logFile) throws IOException {
        this.legacyLogFile = logFile;
        this.directory=logFile.toAbsolutePath().getParent();

        Files.createDirectories(directory);

        if(!Files.exists(legacyLogFile)) {
            Files.createFile(legacyLogFile);
        }

        List<Path> segments=findSegments();
        if(segments.isEmpty()) {
            migrateLegacyLog();
        }
        else {
            recoverSegments(segments);
        }

        if(activeSegment==null) {
            createSegment(nextOffset);
        }
    }

    private List<Path> findSegments() throws IOException {
        try(var paths=Files.list(directory)) {
            return paths
                .filter(path->SEGMENT_PATTERN.matcher(
                        path.getFileName().toString()).matches())
                .sorted(Comparator.comparing(
                        path->path.getFileName().toString()))
                .toList();
        }
    }

    private String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded),StandardCharsets.UTF_8);
    }

    private Path segmentPath(long baseOffset) {
        return directory.resolve(String.format("%020d.log",baseOffset));
    }

    private Path indexPath(Path segment) {
        String name=segment.getFileName().toString();
        return segment.resolveSibling(name.substring(0,name.length()-4)+".index");
    }

    private void createSegment(long baseOffset) throws IOException{
        Path newSegment=segmentPath(baseOffset);
        Path newIndex=indexPath(newSegment);

        //Create both files if they dont exist

        Files.createFile(newSegment);

        try {
            Files.createFile(newIndex);
        }
        catch(IOException e) {
            //Avoid leaving a newly created segment behind if index
            //creation fails

            try {
                Files.deleteIfExists(newSegment);
            }
            catch(IOException cleanupError) {
                e.addSuppressed(cleanupError);
            }

            throw e;
        }

        //update active state only after both files are created
        activeSegment=newSegment;
        activeSegmentSize=0;
    }

    private String serialize(Message message,long offset) {
        return offset+"|"+encode(message.getTopic())+"|"+encode(message.getValue())+"|"+message.getTimestamp().toEpochMilli()+"\n";
    }

    private Message deserialize(String record) throws IOException{
        try {
            String[] parts=record.split("\\|",-1);
            
            if(parts.length!=4) {
                throw new IOException("Invalid log record: "+record);
            }

            long offset=Long.parseLong(parts[0]);
            String topic=decode(parts[1]);
            String value=decode(parts[2]);
            Instant timestamp=Instant.ofEpochMilli(Long.parseLong(parts[3]));

            Message message=new Message(topic,value,timestamp);
            message.setOffset(offset);
            return message;
        }
        catch (IllegalArgumentException e) {
            throw new IOException("Invalid log record format: "+record,e);
        }
    }

    private void migrateLegacyLog() throws IOException {
        List<String> records=Files.readAllLines(legacyLogFile,StandardCharsets.UTF_8);

        for(String record:records) {
            if(record.isBlank()) continue;
            
            Message message=deserialize(record);
            long offset=message.getOffset();

            if(offset!=nextOffset) {
                throw new IOException("Non Contigous legacy offsets: expected "+nextOffset+" ,got "+offset);
            }

            writeRecord(message,offset);
            // messages.add(message);
            nextOffset++;
        }
    }

    private void recoverSegments(List<Path> segments) throws IOException{
        for(Path segment:segments) {
            Path index=indexPath(segment);

            //Rebuild indexes from logfile during recovery
            Files.writeString(index, "",StandardCharsets.UTF_8,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);

            byte[] bytes=Files.readAllBytes(segment);
            int start=0;
            for(int i=0;i<bytes.length;i++) {
                if(bytes[i]!='\n') {
                    continue;
                }

                int end=i;
                if(end>start && bytes[end-1]=='\r') {
                    end--;
                }
                String record=new String(bytes,start,end-start,StandardCharsets.UTF_8);

                if(!record.isBlank()) {
                    Message message=deserialize(record);
                    long offset=message.getOffset();

                    if(offset!=nextOffset) {
                        throw new IOException("Unexpected Offset in "+segment+": expected "+nextOffset+", got "+offset);
                    }

                    if(offset%INDEX_INTERVAL==0) {
                        appendIndex(index,offset,start);
                    }

                    // messages.add(message);
                    nextOffset++;
                }
                start=i+1;
            }
            if(start<bytes.length) {
                try(FileChannel channel=FileChannel.open(segment,StandardOpenOption.WRITE)) {
                    channel.truncate(start);
                    channel.force(true);
                }
            }
            activeSegment=segment;
            activeSegmentSize=Files.size(segment);
        }
    }

    private void appendIndex(Path index,long offset, long position) throws IOException {
        String entry=offset+"|"+position+"\n";
        Files.writeString(index, entry, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void writeRecord(Message message, long offset) throws IOException {
        String record=serialize(message, offset);
        byte[] bytes=record.getBytes(StandardCharsets.UTF_8);

        if(activeSegment==null || (activeSegmentSize>0 && activeSegmentSize+bytes.length>=MAX_SEGMENT_BYTES)) {
            createSegment(offset);
        }

        long position=activeSegmentSize;

        //write the complete record segment
        try(FileChannel channel=FileChannel.open(activeSegment,StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.APPEND)) {
            ByteBuffer buffer=ByteBuffer.wrap(bytes);

            while(buffer.hasRemaining()) {
                channel.write(buffer);
            }

            //Request that record data and metadata be persisted
            channel.force(true);
        }

        //Update in-memory size only after the record write succeds

        activeSegmentSize+=bytes.length;

        //Write the sparse index after the record is persisted.
        //The index can be rebuild from segment files during recovery

        if(offset%INDEX_INTERVAL==0) {
            Path index=indexPath(activeSegment);

            try {
                appendIndex(index, offset, position);
            }
            catch (IOException e) {
                //The message is already Persisted
                //Ignore this index for the current process

                unusableIndexes.add(index);
            }
        }
    }

    public synchronized long append(Message message) throws IOException {
        long offset=nextOffset;

        //Persist first, only update memory after the write succeds
        writeRecord(message, offset);
        message.setOffset(offset);
        // messages.add(message);
        nextOffset++;

        return offset;
    }

    private Path findSegmentForOffset(long offset) throws IOException {
        List<Path> segments=findSegments();
        Path candidate=null;

        for(Path segment:segments) {
            String name=segment.getFileName().toString();
            long baseOffset=Long.parseLong(name.substring(0,name.length()-4));

            if(baseOffset>offset) {
                break;
            }
            candidate=segment;
        }
        return candidate;
    }

    private long findStartPosition(Path segment, long offset) throws IOException{
        Path index=indexPath(segment);

        if(!Files.exists(index) || unusableIndexes.contains(index)) {
            return 0;
        }

        long position=0;

        for(String line:Files.readAllLines(index,StandardCharsets.UTF_8)) {
            if(line.isBlank()) 
                continue;

            String[] parts=line.split("\\|",-1);

            if(parts.length!=2) {
                throw new IOException("Invalid index entry: "+line);
            }

            long indexedOffset=Long.parseLong(parts[0]);
            long indexedPosition=Long.parseLong(parts[1]);

            if(indexedOffset>offset) {
                break;
            }
            position=indexedPosition;
        }
        return position;
    }

    private Message readFromDisk(long offset) throws IOException {
        Path segment=findSegmentForOffset(offset);
        if(segment==null) {
            return null;
        }

        long position=findStartPosition(segment, offset);

        try(RandomAccessFile file=new RandomAccessFile(segment.toFile(),"r")) {
            file.seek(position);

            String line;

            while((line=file.readLine())!=null) {
                if(line.isBlank()) {
                    continue;
                }

                //Log records contain Base64 encoded strings and numeric fields, so the serialized record itself is ASCII

                Message message=deserialize(line);

                if(message.getOffset()==offset) {
                    return message;
                }

                if(message.getOffset()>offset) {
                    return null;
                }
            }
        }
        return null;
    }

    public synchronized Message read(long offset) {
        if(offset<0 || offset>=nextOffset) {
            return null;
        }

        try {
            return readFromDisk(offset);
        }
        catch(IOException e) {
            throw new UncheckedIOException("Failed to read message at offset " + offset, e);
        }
    }

    public synchronized List<Message> readFrom(long offset,int maxBytes) {
        List<Message> result=new ArrayList<>();

        if(offset<0 || offset>=nextOffset || maxBytes<=0)
            return result;

        try {
            List<Path> segments=findSegments();

            Path startingSegment=findSegmentForOffset(offset);

            if(startingSegment==null) {
                return result;
            }

            boolean started=false;
            long totalBytes=0;

            for(Path segment:segments) {
                if(!started) {
                    if(!segment.equals(startingSegment)) {
                        continue;
                    }
                    started=true;
                }

                long position=segment.equals(startingSegment)?findStartPosition(segment, offset):0;

                try(RandomAccessFile file=new RandomAccessFile(segment.toFile(), "r")) {
                    file.seek(position);

                    String line;

                    while((line=file.readLine())!=null) {
                        if(line.isBlank()) {
                            continue;
                        }
                        Message message=deserialize(line);

                        if(message.getOffset()<offset)
                            continue;

                        int messageBytes=serialize(message, message.getOffset()).getBytes(StandardCharsets.UTF_8).length;

                        if(totalBytes+messageBytes>maxBytes) {
                            return result;
                        }

                        result.add(message);
                        totalBytes+=messageBytes;
                    }
                }
            }

            return result;
        }
        catch(IOException e) {
            throw new UncheckedIOException("Failed to read messages from offset"+offset,e);
        }
    }

    public synchronized int size() {
        return Math.toIntExact(nextOffset);
    }
}
