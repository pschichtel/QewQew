/*
 * The MIT License
 * Copyright © 2018 Phillip Schichtel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * queue   := chunk*
 * chunk   := headPtr tailPtr nextRef entry*
 * entry   := length data
 * headPtr := ptr
 * tailPtr := ptr
 * nextRef := '16 bit unsigned integer'
 * ptr     := '32 bit unsigned integer'
 * length  := '16 bit unsigned integer'
 * data    := 'arbitrary many bytes'
 */
public class SimpleQewQew implements QewQew<byte[]> {
    private static final int REF_SIZE = Short.BYTES;
    private static final int PTR_SIZE = Integer.BYTES;
    private static final int QUEUE_HEAD_SIZE = REF_SIZE;
    private static final int CHUNK_HEADER_OFFSET = 0;
    private static final int CHUNK_HEADER_SIZE = PTR_SIZE + PTR_SIZE + REF_SIZE;
    private static final int CHUNK_HEAD_PTR_OFFSET = CHUNK_HEADER_OFFSET;
    private static final int CHUNK_TAIL_PTR_OFFSET = CHUNK_HEAD_PTR_OFFSET + PTR_SIZE;
    private static final int CHUNK_NEXT_REF_OFFSET = CHUNK_TAIL_PTR_OFFSET + PTR_SIZE;
    private static final int ENTRY_HEADER_SIZE = Short.BYTES;

    private static final int NULL_REF = 0;
    private static final int MAX_ID = ((short)-1) & 0xFFFF;
    private static final long MAX_CHUNK_SIZE = 0xFFFFFFFFL;

    private final Head head;
    private final Deque<Chunk> chunks;
    private int cachedHeadSize;

    private final long chunkSize;
    private final byte[] buffer;

    public SimpleQewQew(Path queuePath, int bufferSize, long chunkSize) throws IOException {
        if (chunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException("chunkSize must fit into 32 bits!");
        }

        this.buffer = new byte[bufferSize];

        this.head = openQueue(queuePath, this.buffer);
        this.chunks = loadChunks(this.head, this.buffer, this.head.first);
        this.cachedHeadSize = -1;

        this.chunkSize = chunkSize;
    }

    private static Head openQueue(Path path, byte[] buf) throws IOException {
        final Path absPath = path.toAbsolutePath();
        final RandomAccessFile file = new RandomAccessFile(absPath.toFile(), "rwd");
        final FileLock lock;
        FileChannel channel = file.getChannel();
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            throw new QewAlreadyOpenException(e);
        }
        if (lock == null) {
            throw new QewAlreadyOpenException();
        }
        final int next;
        if (file.length() == 0) {
            next = NULL_REF;
        } else {
            file.readFully(buf, 0, QUEUE_HEAD_SIZE);
            next = getUShort(buf, 0);
        }
        return new Head(absPath, file, lock, next);
    }

    private static Deque<Chunk> loadChunks(Head head, byte[] buf, int next) throws IOException {

        Deque<Chunk> chunks = new ArrayDeque<>();

        while (next != NULL_REF) {
            Chunk chunk = openChunk(head, buf, next, false);
            chunks.addLast(chunk);
            next = chunk.next;
        }

        return chunks;
    }

    private static Chunk openChunk(Head head, byte[] buf, int id, boolean forceNew) throws IOException {
        Path path = resolveNextRef(head, id);
        RandomAccessFile file = open(path);

        if (forceNew) {
            file.setLength(0);
        }

        final long headPtr;
        final long tailPtr;
        final int next;
        if (file.length() == 0) {
            headPtr = CHUNK_HEADER_SIZE;
            tailPtr = CHUNK_HEADER_SIZE;
            next = NULL_REF;
        } else {
            file.seek(CHUNK_HEADER_OFFSET);
            file.readFully(buf, 0, CHUNK_HEADER_SIZE);
            headPtr = getUInt(buf, CHUNK_HEAD_PTR_OFFSET);
            tailPtr = getUInt(buf, CHUNK_TAIL_PTR_OFFSET);
            next = getUShort(buf, CHUNK_NEXT_REF_OFFSET);
        }

        return new Chunk(path, file, headPtr, tailPtr, id, next);
    }

    private static RandomAccessFile open(Path path) throws IOException {
        if (Files.notExists(path)) {
            Files.createFile(path);
        }
        return new RandomAccessFile(path.toFile(), "rwd");
    }

    private static Path resolveNextRef(Head head, int id) {
        Path parent = head.path.getParent();
        String name = head.path.getFileName().toString();
        return parent.resolve(name + "." + (id % MAX_ID));
    }

    public boolean isEmpty() {
        if (chunks.isEmpty()) {
            return true;
        }
        if (chunks.size() == 1) {
            Chunk c = chunks.getFirst();
            return c.headPtr >= c.tailPtr;
        }
        return false;
    }

    public boolean clear() throws IOException {
        if (isEmpty()) {
            return false;
        }
        head.first = NULL_REF;
        writeQueueFirst(head, buffer);
        Iterator<Chunk> it = chunks.iterator();
        Chunk first = it.next();
        resetChunk(first, buffer);
        while (it.hasNext()) {
            it.next().drop();
            it.remove();
        }
        return true;
    }

    public int peekLength() throws IOException {
        return peekLength(chunks.getFirst(), buffer);
    }

    public int peek(byte[] output) throws IOException {
        return peek(output, 0, output.length);
    }

    public int peek(byte[] output, int offset, int length) throws IOException {
        Chunk head = chunks.getFirst();
        return peek(head, output, offset, length, false);
    }

    public byte[] peek() throws IOException {
        if (isEmpty()) {
            return null;
        }

        // TODO try to merge the 2 reads into one


        Chunk head = chunks.getFirst();
        byte[] output = new byte[peekLength(head, buffer)];
        peek(head, output, 0, output.length, true);
        return output;
    }

    private static int peek(Chunk chunk, byte[] output, int offset, int length, boolean fully) throws IOException {
        final long fileBase = chunk.headPtr + ENTRY_HEADER_SIZE;
        chunk.file.seek(fileBase);
        if (fully) {
            chunk.file.readFully(output, offset, length);
            return -1;
        } else {
            return chunk.file.read(output, offset, length);
        }
    }

    private int peekLength(Chunk chunk, byte[] buf) throws IOException {
        if (cachedHeadSize == -1) {
            chunk.file.seek(chunk.headPtr);
            chunk.file.readFully(buf, 0, ENTRY_HEADER_SIZE);
            cachedHeadSize = getUShort(buf, 0);
        }
        return cachedHeadSize;
    }

    public boolean dequeue() throws IOException {

        if (isEmpty()) {
            return false;
        }

        Chunk chunk = chunks.getFirst();
        int length = peekLength(chunk, buffer);
        cachedHeadSize = -1;
        chunk.headPtr = chunk.headPtr + ENTRY_HEADER_SIZE + length;

        if (chunk.headPtr >= chunk.tailPtr) {
            if (chunks.size() == 1) {
                Chunk onlyRemaining = chunks.getFirst();
                resetChunk(onlyRemaining, buffer);
            } else {
                writeQueueFirst(head, buffer);
                Chunk depleted = chunks.removeFirst();
                depleted.drop();
                head.first = depleted.next;
            }
        } else {
            writeChunkHeadPtr(chunk, buffer);
        }
        return true;
    }

    public void enqueue(byte[] input) throws IOException {
        enqueue(input, 0, input.length);
    }

    public void enqueue(byte[] input, int offset, int length) throws IOException {
        boolean newChunk = false;
        Chunk chunk;
        if (chunks.isEmpty()) {
            chunk = openChunk(this.head, this.buffer, 1, true);
            head.first = chunk.id;
            writeQueueFirst(head, buffer);
            chunks.addLast(chunk);
            newChunk = true;
            cachedHeadSize = input.length;
        } else {
            chunk = chunks.getLast();
        }

        writeToChunk(chunk, this.buffer, input, offset, length, newChunk);
    }

    @Override
    public void close() throws IOException {
        for (Chunk chunk : chunks) {
            try {
                chunk.close();
            } catch (IOException ignored) {

            }
        }
        head.close();
        if (isEmpty()) {
            for (Chunk chunk : chunks) {
                try {
                    chunk.drop();
                } catch (IOException ignored) {

                }
            }
            Files.delete(head.path);
        }
    }

    private void writeToChunk(Chunk chunk, byte[] buf, byte[] payload, int offset, int length, boolean newChunk) throws IOException {
        while (chunk.tailPtr + payload.length >= chunkSize) {
            int nextId = (chunk.id + 1) % MAX_ID;
            if (nextId == 0) {
                nextId++;
            }
            chunk.next = nextId;
            writeChunkNextRef(chunk, buf);
            Chunk next = openChunk(this.head, this.buffer, nextId, true);
            chunks.addLast(next);
            chunk = next;
            newChunk = true;
        }
        chunk.file.seek(chunk.tailPtr);
        putUShort(buf, 0, payload.length);
        chunk.file.write(buf, 0, ENTRY_HEADER_SIZE);
        chunk.file.write(payload, offset, length);

        chunk.tailPtr = chunk.tailPtr + ENTRY_HEADER_SIZE + payload.length;
        if (newChunk) {
            writeChunkHeader(chunk, buf);
        } else {
            writeChunkTailPtr(chunk, buf);
        }
    }

    private static void resetChunk(Chunk chunk, byte[] buf) throws IOException {
        chunk.headPtr = CHUNK_HEADER_SIZE;
        chunk.tailPtr = CHUNK_HEADER_SIZE;
        chunk.next = NULL_REF;
        writeChunkHeader(chunk, buf);
    }

    private static void writeChunkHeader(Chunk chunk, byte[] buf) throws IOException {
        putUInt(buf, CHUNK_HEAD_PTR_OFFSET, chunk.headPtr);
        putUInt(buf, CHUNK_TAIL_PTR_OFFSET, chunk.tailPtr);
        putUShort(buf, CHUNK_NEXT_REF_OFFSET, chunk.next);
        chunk.file.seek(CHUNK_HEADER_OFFSET);
        chunk.file.write(buf, 0, CHUNK_HEADER_SIZE);
    }

    private static void writeChunkHeadPtr(Chunk chunk, byte[] buf) throws IOException {
        chunk.file.seek(CHUNK_HEAD_PTR_OFFSET);
        putUInt(buf, 0, chunk.headPtr);
        chunk.file.write(buf, 0, PTR_SIZE);
    }

    private static void writeChunkTailPtr(Chunk chunk, byte[] buf) throws IOException {
        chunk.file.seek(CHUNK_TAIL_PTR_OFFSET);
        putUInt(buf, 0, chunk.tailPtr);
        chunk.file.write(buf, 0, PTR_SIZE);
    }


    private static void writeChunkNextRef(Chunk chunk, byte[] buf) throws IOException {
        chunk.file.seek(CHUNK_NEXT_REF_OFFSET);
        putUShort(buf, 0, chunk.next);
        chunk.file.write(buf, 0, REF_SIZE);
    }

    private static void writeQueueFirst(Head head, byte[] buf) throws IOException {
        head.file.seek(0);
        putUShort(buf, 0, head.first);
        head.file.write(buf, 0, REF_SIZE);
    }

    private static void putUShort(byte[] buf, int offset, int i) {
        int value = i & 0xFFFF;
        buf[offset    ] = (byte) (value >> 8);
        buf[offset + 1] = (byte) value;
    }

    private static int getUShort(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xFF) << 8) + (buffer[offset + 1] & 0xFF);
    }

    private static void putUInt(byte[] buf, int offset, long i) {
        int value = (int) (i & 0xFFFFFFFFL);
        buf[offset    ] = (byte) (value >> 24);
        buf[offset + 1] = (byte) (value >> 16);
        buf[offset + 2] = (byte) (value >> 8);
        buf[offset + 3] = (byte) value;
    }

    /** Reads an {@code int} from the {@code byte[]}. */
    private static long getUInt(byte[] buffer, int offset) {
        return ((buffer[offset    ] & 0xFFL) << 24)
                +  ((buffer[offset + 1] & 0xFFL) << 16)
                +  ((buffer[offset + 2] & 0xFFL) << 8)
                +   (buffer[offset + 3] & 0xFFL);
    }

    private static final class Head implements Closeable {
        public final Path path;
        public final RandomAccessFile file;
        public final FileLock lock;
        public int first;

        public Head(Path path, RandomAccessFile file, FileLock lock, int first) throws IOException {
            this.path = path;
            this.file = file;
            this.lock = lock;
            this.first = first;
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }

    private static final class Chunk implements Closeable {
        public final Path path;
        public final RandomAccessFile file;
        public long headPtr;
        public long tailPtr;
        public final int id;
        public int next;

        public Chunk(Path path, RandomAccessFile file, long headPtr, long tailPtr, int id, int next) throws IOException {
            file.getChannel().lock();
            this.path = path;
            this.file = file;
            this.id = id;
            this.headPtr = headPtr;
            this.tailPtr = tailPtr;
            this.next = next;
        }

        public void drop() throws IOException {
            close();
            Files.delete(path);
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }
}
