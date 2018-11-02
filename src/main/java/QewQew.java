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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
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
 * ptr     := '64 bit signed integer'
 * length  := '16 bit unsigned integer'
 * data    := 'arbitrary many bytes'
 */
public class QewQew implements Closeable {
    private static final int CHUNK_HEADER_SIZE = Long.BYTES + Long.BYTES + Short.BYTES;
    private static final int ENTRY_HEADER_SIZE = Short.BYTES;
    private static final int NULL_REF = 0;
    private static final String SUFFIX = ".qew";
    private static final int MAX_ID = ((short)-1) & 0xFFFF;

    private final Path prefix;
    private Path headPath;

    private FileChannel headFile;
    private final Lock headLock;
    private FileChannel tailFile;
    private final Lock tailLock;

    private final long chunkSize;
    private final long blockSize;
    private final ByteBuffer headBuffer;
    private final ByteBuffer tailBuffer;

    public QewQew(Path headPath, int blockSize, long chunkSize, boolean fair) throws IOException {

        this.headLock = new ReentrantLock(fair);
        this.tailLock = new ReentrantLock(fair);

        this.headBuffer = ByteBuffer.allocateDirect(blockSize);
        this.tailBuffer = ByteBuffer.allocateDirect(blockSize);

        this.prefix = headPath.toAbsolutePath().getParent();
        this.blockSize = blockSize;
        this.chunkSize = chunkSize;
        this.headPath = headPath;
        this.headFile = openChunk(this.headPath);

        this.tailFile = findTail(prefix, this.headFile, this.headBuffer);
    }

    private static int readNextRef(FileChannel file, ByteBuffer buf) throws IOException {
        if (file.size() == 0) {
            return NULL_REF;
        }
        buf.clear().limit(CHUNK_HEADER_SIZE);
        file.position(0);
        file.read(buf);
        buf.flip();
        buf.getLong();
        buf.getLong();
        return buf.getShort();
    }

    private static FileChannel findTail(Path prefix, FileChannel file, ByteBuffer buf) throws IOException {
        try (FileLock ignored = file.lock()) {
            int nextRef = readNextRef(file, buf);
            Path next = resolveNextRef(prefix, nextRef);
            FileChannel nextFile = tryOpenChunk(next);
            if (nextFile == null) {
                return file;
            } else {
                // it is important here to continue the search file the current file is still locked to prevent
                // queue changes while search.
                return findTail(prefix, nextFile, buf);
            }
        }
    }

    private static Path resolveNextRef(Path prefix, int id) {
        return prefix.resolve((id % MAX_ID) + SUFFIX);
    }

    private static FileChannel openChunk(Path path) throws IOException {
        return FileChannel.open(path, CREATE, WRITE, READ, SYNC, DSYNC);
    }

    private static FileChannel tryOpenChunk(Path path) throws IOException {
        try {
            return FileChannel.open(path, WRITE, READ, SYNC, DSYNC);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    public boolean isEmpty() throws IOException {
        headLock.lock();
        try {
            try (FileLock ignored = headFile.lock()) {
                return findHeadPtr() == -1;
            }
        } finally {
            headLock.unlock();
        }
    }

    public void clear() throws IOException {
        headLock.lock();
        try {
            try (FileLock ignored = headFile.lock()) {
                headFile.truncate(0);
            }
        } finally {
            headLock.unlock();
        }
    }

    @ExpectsValidLock
    private long findHeadPtr() throws IOException {
        if (headFile.size() == 0) {
            return -1;
        }

        headFile.position(0);
        headBuffer.clear().limit(CHUNK_HEADER_SIZE);
        headFile.read(headBuffer);
        headBuffer.flip();

        long headPtr = headBuffer.getLong();
        long tailPtr = headBuffer.getLong();
        return headPtr < tailPtr ? headPtr : -1;
    }

    private FileLock lockNonEmptyHead() throws IOException {
        long headPtr;
        long tailPtr;
        int nextRef;
        FileLock lock = headFile.lock();
        while (true) {
            if (headFile.size() == 0) {
                break;
            }
            headFile.position(0);
            headBuffer.clear().limit(CHUNK_HEADER_SIZE);
            headFile.read(headBuffer);
            headBuffer.flip();
            headPtr = headBuffer.getLong();
            tailPtr = headBuffer.getLong();
            if (headPtr < tailPtr) {
                break;
            }

            nextRef = headBuffer.getShort() & 0xFFFF;
            Path next = resolveNextRef(prefix, nextRef);
            try {
                Files.move(next, headPath, ATOMIC_MOVE);
                this.headFile = openChunk(headPath);
                lock = this.headFile.lock();
            } catch (NoSuchFileException ignored) {
                // next does not exist,
                break;
            }
        }
        return lock;
    }

    public byte[] peek() throws IOException {
        final FileLock currentLock = lockNonEmptyHead();
        try {
            long headPtr = findHeadPtr();
            if (headPtr == -1) {
                return null;
            }

            return readEntry(headFile, headBuffer, headPtr);
        } finally {
            if (currentLock.isValid()) {
                currentLock.release();
            }
        }
    }

    @ExpectsValidLock
    private static byte[] readEntry(FileChannel file, ByteBuffer buf, long ptr) throws IOException {
        file.position(ptr);

        buf.clear().limit(ENTRY_HEADER_SIZE);
        file.read(buf);
        buf.flip();
        final int length = buf.getShort() & 0xFFFF;

        int offset = 0;
        byte[] output = new byte[length];
        while (offset < length) {
            buf.clear();
            buf.limit(Math.min(length - offset, buf.capacity()));
            file.read(buf);
            buf.flip();
            buf.get(output, offset, buf.limit());
            offset += buf.limit();
        }

        return output;
    }

    public boolean deqew() throws IOException {
        headLock.lock();
        try {
            try (FileLock ignored = headFile.lock()) {
                headFile.position(0);
                headBuffer.clear().limit(CHUNK_HEADER_SIZE);
                headFile.read(headBuffer);
                headBuffer.flip();
                long headPtr = headBuffer.getLong();
                long tailPtr = headBuffer.getLong();
                int nextRef = headBuffer.getShort() & 0xFFFF;
                if (headPtr >= tailPtr) {
                    // TODO delete empty head
                    return false;
                }
                headFile.position(headPtr);

                headBuffer.clear().limit(ENTRY_HEADER_SIZE);
                headFile.read(headBuffer);
                headBuffer.flip();
                final int length = headBuffer.getShort() & 0xFFFF;
                updateHeader(headFile, headBuffer, headFile.position() + length, tailPtr, nextRef);
                return true;
            }
        } finally {
            headLock.unlock();
        }
    }

    public void enqew(byte[] input) throws IOException {
        tailLock.lock();
        try {
            try (FileLock ignored = tailFile.lock()) {
                final long headPtr;
                final long tailPtr;
                final int nextRef;
                if (tailFile.size() == 0) {
                    headPtr = CHUNK_HEADER_SIZE;
                    tailPtr = CHUNK_HEADER_SIZE;
                    nextRef = NULL_REF;
                    tailFile.truncate(chunkSize * blockSize);
                } else {
                    tailFile.position(0);
                    tailBuffer.clear().limit(CHUNK_HEADER_SIZE);
                    tailFile.read(tailBuffer);
                    tailBuffer.flip();
                    headPtr = tailBuffer.getLong();
                    tailPtr = tailBuffer.getLong();
                    nextRef = tailBuffer.getShort() & 0xFFFF;
                }
                tailFile.position(tailPtr);

                tailBuffer.clear();
                tailBuffer.putShort((short) (input.length & 0xFFFF));
                tailBuffer.flip();
                tailFile.write(tailBuffer);

                int offset = 0;
                while (offset < input.length) {
                    tailBuffer.clear();
                    tailBuffer.put(input, offset, Math.min(tailBuffer.capacity(), input.length - offset));
                    tailBuffer.flip();
                    tailFile.write(tailBuffer);
                    offset += tailBuffer.limit();
                }

                updateHeader(tailFile, tailBuffer, headPtr, tailFile.position(), nextRef);
            }
        } finally {
            tailLock.unlock();
        }
    }

    private FileLock lockTail(int forBytes) throws IOException {
        long headPtr;
        long tailPtr;
        int nextRef;
        FileLock lock = tailFile.lock();
        while (true) {
            if (headFile.size() == 0) {
                break;
            }
            headFile.position(0);
            headBuffer.clear().limit(CHUNK_HEADER_SIZE);
            headFile.read(headBuffer);
            headBuffer.flip();
            headPtr = headBuffer.getLong();
            tailPtr = headBuffer.getLong();
            nextRef = headBuffer.getShort() & 0xFFFF;
            if (nextRef != NULL_REF && tailPtr + forBytes < tailFile.size()) {
                break;
            }
            Path next = resolveNextRef(prefix, nextRef);
            try {
                Files.move(next, headPath, ATOMIC_MOVE);
                this.headFile = openChunk(headPath);
                lock = this.headFile.lock();
            } catch (NoSuchFileException ignored) {
                // next does not exist,
                break;
            }
        }
        return lock;
    }

    private static void updateHeader(FileChannel ch, ByteBuffer buf, long headPtr, long tailPtr, int next) throws IOException {
        short wrappedNext = (short) (next & 0xFFFF);
        if (wrappedNext == NULL_REF) {
            wrappedNext++;
        }
        ch.position(0);
        buf.clear();
        buf.putLong(headPtr);
        buf.putLong(tailPtr);
        buf.putShort(wrappedNext);
        buf.flip();
        ch.write(buf);
    }

    @Override
    public void close() throws IOException {
        headLock.lock();
        tailLock.lock();
        try {
            try {
                this.headFile.close();
            } finally {
                this.tailFile.close();
            }
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
    }
}
