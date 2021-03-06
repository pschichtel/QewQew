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
package tel.schich.qewqew;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
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
    static final int REF_SIZE = Short.BYTES;
    static final int PTR_SIZE = Integer.BYTES;
    static final int QUEUE_HEAD_SIZE = REF_SIZE;
    static final int CHUNK_HEADER_OFFSET = 0;
    static final int CHUNK_HEADER_SIZE = PTR_SIZE + PTR_SIZE + REF_SIZE;
    static final int CHUNK_HEAD_PTR_OFFSET = CHUNK_HEADER_OFFSET;
    static final int CHUNK_TAIL_PTR_OFFSET = CHUNK_HEAD_PTR_OFFSET + PTR_SIZE;
    static final int CHUNK_NEXT_REF_OFFSET = CHUNK_TAIL_PTR_OFFSET + PTR_SIZE;
    static final int ENTRY_HEADER_SIZE = Short.BYTES;

    static final int NULL_REF = 0;
    private static final int MAX_ID = ((short)-1) & 0xFFFF;
    private static final long MAX_CHUNK_SIZE = 0xFFFFFFFFL;

    private final Head head;
    private final Deque<Chunk> chunks;
    private int cachedHeadSize;

    private final long chunkSize;

    public SimpleQewQew(Path queuePath, long chunkSize) throws IOException {
        if (chunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException("chunkSize must fit into 32 bits!");
        }

        this.head = openQueue(queuePath);
        this.chunks = loadChunks(this.head, chunkSize);
        this.cachedHeadSize = -1;

        this.chunkSize = chunkSize;
    }

    public int countChunks() {
        if (isEmpty()) {
            return 0;
        } else {
            return this.chunks.size();
        }
    }

    @Override
    public long getChunkSize() {
        return chunkSize;
    }

    @Override
    public long getMaxElementSize() {
        return getChunkSize() - CHUNK_HEADER_SIZE - ENTRY_HEADER_SIZE;
    }

    private static Head openQueue(Path path) throws IOException {
        final Path absPath = path.toAbsolutePath();
        final FileChannel file = openFile(absPath);
        final FileLock lock;
        try {
            lock = file.tryLock();
        } catch (OverlappingFileLockException e) {
            throw new QewAlreadyOpenException(e);
        }
        if (lock == null) {
            throw new QewAlreadyOpenException();
        }
        file.truncate(QUEUE_HEAD_SIZE);
        final MappedByteBuffer map = file.map(FileChannel.MapMode.READ_WRITE, 0, QUEUE_HEAD_SIZE);
        int next = getUShort(map, 0);
        return new Head(absPath, file, lock, map, next);
    }

    private static Deque<Chunk> loadChunks(Head head, long chunkSize) throws IOException {

        int next = head.first;
        Deque<Chunk> chunks = new ArrayDeque<>();

        while (next != NULL_REF) {
            Chunk chunk = openChunk(head, next, false, chunkSize);
            chunks.addLast(chunk);
            next = chunk.next;
        }

        return chunks;
    }

    private static Chunk openChunk(Head head, int id, boolean forceNew, long chunkSize) throws IOException {
        final Path path = resolveNextRef(head, id);
        return new Chunk(path, id, chunkSize).init(forceNew);
    }

    static FileChannel openFile(Path path) throws IOException {
        return FileChannel.open(path, CREATE, WRITE, READ, DSYNC);
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
        writeQueueFirst(head);
        Iterator<Chunk> it = chunks.iterator();
        Chunk first = it.next();
        resetChunk(first);
        while (it.hasNext()) {
            it.next().drop();
            it.remove();
        }
        return true;
    }

    public int peekLength() {
        return peekLength(chunks.getFirst());
    }

    public void peek(byte[] output) {
        Chunk head = chunks.getFirst();
        head.peek(output);
    }

    public byte[] peek() {
        if (isEmpty()) {
            return null;
        }

        Chunk head = chunks.getFirst();
        byte[] output = new byte[peekLength(head)];
        return head.peek(output);
    }

    public boolean dequeue() throws IOException {

        if (isEmpty()) {
            return false;
        }

        Chunk chunk = chunks.getFirst();
        int length = peekLength(chunk);
        cachedHeadSize = -1;
        chunk.headPtr = chunk.headPtr + ENTRY_HEADER_SIZE + length;

        if (chunk.headPtr >= chunk.tailPtr) {
            if (chunks.size() == 1) {
                Chunk onlyRemaining = chunks.getFirst();
                resetChunk(onlyRemaining);
                chunk.force();
            } else {
                Chunk depleted = chunks.removeFirst();
                depleted.drop();
                head.first = depleted.next;
                chunks.getFirst().open(); // open next chunk
                writeQueueFirst(head);
            }
        } else {
            chunk.writeChunkHeadPtr();
            chunk.force();
        }

        return true;
    }

    private int peekLength(Chunk chunk) {
        if (cachedHeadSize == -1) {
            cachedHeadSize = chunk.peekLength();
        }
        return cachedHeadSize;
    }

    public void enqueue(byte[] input) throws IOException, BufferOverflowException {
        enqueue(input, 0, input.length);
    }

    public void enqueue(byte[] input, int offset, int length) throws IOException {
        boolean newChunk = false;
        Chunk chunk;
        if (chunks.isEmpty()) {
            chunk = openChunk(this.head, 1, true, this.chunkSize);
            head.first = chunk.id;
            writeQueueFirst(head);
            chunks.addLast(chunk);
            newChunk = true;
            cachedHeadSize = input.length;
        } else {
            chunk = chunks.getLast();
        }

        writeToChunk(chunk, input, offset, length, newChunk);
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
            try {
                Files.delete(head.path);
            } catch (AccessDeniedException ignored) {
                // FUCK OFF Windows
                head.path.toFile().deleteOnExit();
            }
        }
    }

    private void writeToChunk(Chunk chunk, byte[] payload, int offset, int length, boolean newChunk) throws IOException {
        if (chunk.tailPtr + ENTRY_HEADER_SIZE + length > chunkSize) {
            int nextId = (chunk.id + 1) % MAX_ID;
            if (nextId == 0) {
                nextId++;
            }
            chunk.next = nextId;
            chunk.writeChunkNextRef();
            Chunk next = openChunk(this.head, nextId, true, this.chunkSize);
            chunk.close();
            chunks.addLast(next);
            chunk = next;
            newChunk = true;
        }

        chunk.putPayload(payload, offset, length);

        chunk.tailPtr = chunk.tailPtr + ENTRY_HEADER_SIZE + length;
        if (newChunk) {
            chunk.writeChunkHeader();
        } else {
            chunk.writeChunkTailPtr();
        }
        chunk.force();
    }

    private static void resetChunk(Chunk chunk) {
        chunk.headPtr = CHUNK_HEADER_SIZE;
        chunk.tailPtr = CHUNK_HEADER_SIZE;
        chunk.next = NULL_REF;
        chunk.writeChunkHeader();
    }


    private static void writeQueueFirst(Head head) {
        putUShort(head.map, 0, head.first);
        head.map.force();
    }

    static int getUShort(ByteBuffer buf, int index) {
        return buf.getShort(index) & 0xFFFF;
    }

    static void putUShort(ByteBuffer buf, int index, int i) {
        buf.putShort(index, (short) (i & 0xFFFF));
    }

}
