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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

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
    private static final String SUFFIX = ".qew";
    private static final int MAX_ID = ((short)-1) & 0xFFFF;
    private static final long MAX_CHUNK_SIZE = 0xFFFFFFFFL;

    private final Path prefix;

    private final Head head;
    private final Deque<Chunk> chunks;
    private int cachedHeadSize;

    private final long chunkSize;
    private final ByteBuffer headBuffer;
    private final ByteBuffer tailBuffer;

    public SimpleQewQew(Path queuePath, int bufferSize, long chunkSize) throws IOException {
        if (chunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException("chunkSize must fit into 32 bits!");
        }

        this.headBuffer = ByteBuffer.allocateDirect(ENTRY_HEADER_SIZE + bufferSize);
        this.tailBuffer = ByteBuffer.allocateDirect(ENTRY_HEADER_SIZE + bufferSize);

        this.head = openQueue(queuePath, headBuffer);
        this.prefix = queuePath.toAbsolutePath().getParent();
        this.chunks = loadChunks(this.prefix, headBuffer, this.head.first);
        this.cachedHeadSize = -1;

        this.chunkSize = chunkSize;
    }

    private static Head openQueue(Path path, ByteBuffer buf) throws IOException {
        final FileChannel file = FileChannel.open(path, CREATE, WRITE, READ, SYNC, DSYNC);
        final FileLock lock;
        try {
            lock = file.tryLock();
        } catch (OverlappingFileLockException e) {
            throw new QewAlreadyOpenException(e);
        }
        if (lock == null) {
            throw new QewAlreadyOpenException();
        }
        final int next;
        if (file.size() == 0) {
            next = NULL_REF;
        } else {
            buf.clear().limit(QUEUE_HEAD_SIZE);
            file.read(buf);
            buf.flip();
            next = getUShort(buf);
        }
        return new Head(path, file, lock, next);
    }

    private static Deque<Chunk> loadChunks(Path prefix, ByteBuffer buf, int next) throws IOException {

        Deque<Chunk> chunks = new ArrayDeque<>();

        while (next != NULL_REF) {
            Chunk chunk = openChunk(prefix, buf, next, false);
            chunks.addLast(chunk);
            next = chunk.next;
        }

        return chunks;
    }

    private static Chunk openChunk(Path prefix, ByteBuffer buf, int id, boolean forceNew) throws IOException {
        Path path = resolveNextRef(prefix, id);
        FileChannel file = FileChannel.open(path, CREATE, WRITE, READ, SYNC, DSYNC);

        if (forceNew) {
            file.truncate(0);
        }

        final long headPtr;
        final long tailPtr;
        final int next;
        if (file.size() == 0) {
            headPtr = CHUNK_HEADER_SIZE;
            tailPtr = CHUNK_HEADER_SIZE;
            next = NULL_REF;
        } else {
            buf.clear();
            buf.limit(CHUNK_HEADER_SIZE);
            file.read(buf, 0);
            buf.flip();
            headPtr = getUInt(buf);
            tailPtr = getUInt(buf);
            next = getUShort(buf);
        }

        return new Chunk(path, file, headPtr, tailPtr, id, next);
    }

    private static Path resolveNextRef(Path prefix, int id) {
        return prefix.resolve((id % MAX_ID) + SUFFIX);
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
        writeQueueFirst(head, headBuffer);
        Iterator<Chunk> it = chunks.iterator();
        Chunk first = it.next();
        resetChunk(first, headBuffer);
        while (it.hasNext()) {
            it.next().drop();
            it.remove();
        }
        return true;
    }

    public int peekLength() throws IOException {
        return peekLength(chunks.getFirst(), headBuffer);
    }

    public void peek(byte[] output) throws IOException {
        Chunk head = chunks.getFirst();
        peek(head, headBuffer, output);
    }

    public byte[] peek() throws IOException {
        if (isEmpty()) {
            return null;
        }

        // TODO try to merge the 2 reads into one


        Chunk head = chunks.getFirst();
        byte[] output = new byte[peekLength(head, headBuffer)];
        peek(head, headBuffer, output);
        return output;
    }

    private static void peek(Chunk chunk, ByteBuffer input, byte[] output) throws IOException {
        int offset = 0;
        final long fileBase = chunk.headPtr + ENTRY_HEADER_SIZE;
        while (offset < output.length) {
            input.clear();
            input.limit(Math.min(output.length - offset, input.capacity()));
            int bytesRead = chunk.file.read(input, fileBase + offset);
            input.flip();
            input.get(output, offset, input.limit());
            offset += bytesRead;
        }
    }

    private int peekLength(Chunk chunk, ByteBuffer buf) throws IOException {
        if (cachedHeadSize == -1) {
            buf.clear().limit(ENTRY_HEADER_SIZE);
            chunk.file.read(buf, chunk.headPtr);
            buf.flip();
            cachedHeadSize = getUShort(buf);
        }
        return cachedHeadSize;
    }

    public boolean dequeue() throws IOException {

        if (isEmpty()) {
            return false;
        }

        Chunk chunk = chunks.getFirst();
        int length = peekLength(chunk, headBuffer);
        cachedHeadSize = -1;
        chunk.headPtr = chunk.headPtr + ENTRY_HEADER_SIZE + length;

        if (chunk.headPtr >= chunk.tailPtr) {
            if (chunks.size() == 1) {
                Chunk onlyRemaining = chunks.getFirst();
                resetChunk(onlyRemaining, headBuffer);
            } else {
                writeChunkHeadPtr(chunk, headBuffer);
                Chunk depleted = chunks.removeFirst();
                depleted.drop();
                head.first = depleted.next;
                writeQueueFirst(head, headBuffer);
            }
        } else {
            writeChunkHeadPtr(chunk, headBuffer);
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
            chunk = openChunk(prefix, tailBuffer, 1, true);
            head.first = chunk.id;
            writeQueueFirst(head, tailBuffer);
            chunks.addLast(chunk);
            newChunk = true;
            cachedHeadSize = input.length;
        } else {
            chunk = chunks.getLast();
        }

        writeToChunk(chunk, tailBuffer, input, offset, length, newChunk);
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

    private void writeToChunk(Chunk chunk, ByteBuffer buf, byte[] payload, int offset, int length, boolean newChunk) throws IOException {
        while (chunk.tailPtr + payload.length >= chunkSize) {
            int nextId = (chunk.id + 1) % MAX_ID;
            if (nextId == 0) {
                nextId++;
            }
            chunk.next = nextId;
            writeChunkNextRef(chunk, buf);
            Chunk next = openChunk(prefix, buf, nextId, true);
            chunks.addLast(next);
            chunk = next;
            newChunk = true;
        }
        buf.clear();
        putUShort(buf, payload.length);
        buf.put(payload, offset, Math.min(length, buf.capacity() - ENTRY_HEADER_SIZE));
        buf.flip();
        int bytesWritten = chunk.file.write(buf, chunk.tailPtr);
        while (bytesWritten < length) {
            buf.clear();
            buf.put(payload, offset + bytesWritten, Math.min(length - bytesWritten, buf.capacity() - ENTRY_HEADER_SIZE));
            buf.flip();
            bytesWritten += chunk.file.write(buf, chunk.tailPtr + bytesWritten);
        }

        chunk.tailPtr = chunk.tailPtr + bytesWritten;
        if (newChunk) {
            writeChunkHeader(chunk, buf);
        } else {
            writeChunkTailPtr(chunk, buf);
        }
    }

    private static void resetChunk(Chunk chunk, ByteBuffer buf) throws IOException {
        chunk.headPtr = CHUNK_HEADER_SIZE;
        chunk.tailPtr = CHUNK_HEADER_SIZE;
        chunk.next = NULL_REF;
        writeChunkHeader(chunk, buf);
    }

    private static void writeChunkHeader(Chunk chunk, ByteBuffer buf) throws IOException {
        buf.clear();
        putUInt(buf, chunk.headPtr);
        putUInt(buf, chunk.tailPtr);
        putUShort(buf, chunk.next);
        buf.flip();
        chunk.file.write(buf, CHUNK_HEADER_OFFSET);
    }

    private static void writeChunkHeadPtr(Chunk chunk, ByteBuffer buf) throws IOException {
        buf.clear();
        putUInt(buf, chunk.headPtr);
        buf.flip();
        chunk.file.write(buf, CHUNK_HEAD_PTR_OFFSET);
    }

    private static void writeChunkTailPtr(Chunk chunk, ByteBuffer buf) throws IOException {
        buf.clear();
        putUInt(buf, chunk.tailPtr);
        buf.flip();
        chunk.file.write(buf, CHUNK_TAIL_PTR_OFFSET);
    }


    private static void writeChunkNextRef(Chunk chunk, ByteBuffer buf) throws IOException {
        buf.clear();
        putUShort(buf, chunk.next);
        buf.flip();
        chunk.file.write(buf, CHUNK_NEXT_REF_OFFSET);
    }

    private static void writeQueueFirst(Head head, ByteBuffer buf) throws IOException {
        buf.clear();
        putUShort(buf, head.first);
        buf.flip();
        head.file.write(buf, 0);
    }

    private static int getUShort(ByteBuffer buf) {
        return buf.getShort() & 0xFFFF;
    }

    private static void putUShort(ByteBuffer buf, int i) {
        buf.putShort((short) (i & 0xFFFF));
    }

    private static long getUInt(ByteBuffer buf) {
        return buf.getInt() & 0xFFFFFFFFL;
    }

    private static void putUInt(ByteBuffer buf, long i) {
        buf.putInt((int) (i & 0xFFFFFFFFL));
    }

    private static final class Head implements Closeable {
        public final Path path;
        public final FileChannel file;
        public final FileLock lock;
        public int first;

        public Head(Path path, FileChannel file, FileLock lock, int first) throws IOException {
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
        public final FileChannel file;
        public long headPtr;
        public long tailPtr;
        public final int id;
        public int next;

        public Chunk(Path path, FileChannel file, long headPtr, long tailPtr, int id, int next) throws IOException {
            file.lock();
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
