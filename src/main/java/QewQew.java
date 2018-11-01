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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

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

    private final Path prefix;
    private Path headPath;
    private Path tailPath;

    private FileChannel headFile;
    private FileChannel tailFile;

    private final long chunkSize;
    private final long blockSize;
    private final ByteBuffer headBuffer;
    private final ByteBuffer tailBuffer;

    public QewQew(Path prefix, int blockSize, long chunkSize) throws IOException {
        this.prefix = prefix;
        this.blockSize = blockSize;
        this.chunkSize = chunkSize;
        this.headPath = findHead(prefix);
        this.tailPath = findTail(prefix);

        this.headBuffer = ByteBuffer.allocateDirect(blockSize);
        this.tailBuffer = ByteBuffer.allocateDirect(blockSize);
        this.headFile = openChunk(this.headPath);
        if (this.headPath.equals(this.tailPath)) {
            this.tailFile = this.headFile;
        } else {
            this.tailFile = openChunk(this.tailPath);
        }
    }

    private static Path findHead(Path prefix) throws IOException {
        Optional<Path> first = findFiles(prefix).min(Path::compareTo);
        return first.orElseGet(() -> initial(prefix));
    }

    private static Path findTail(Path prefix) throws IOException {
        Optional<Path> first = findFiles(prefix).min((a, b) -> -a.compareTo(b));
        return first.orElseGet(() -> initial(prefix));
    }

    private static Path initial(Path prefix) {
        return prefix.resolve("1" + SUFFIX);
    }

    private static Stream<Path> findFiles(Path prefix) throws IOException {
        return Files.list(prefix).filter(QewQew::isQueueFile);
    }

    private static boolean isQueueFile(Path p) {
        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }

        return p.endsWith(SUFFIX);
    }

    private static FileChannel openChunk(Path path) throws IOException {
        return FileChannel.open(path, CREATE, WRITE, READ, SYNC, DSYNC);
    }

    public boolean isEmpty() throws IOException {
        try (FileLock ignored = headFile.lock()) {
            if (headFile.size() == 0) {
                return true;
            }
            headFile.position(0);
            headBuffer.clear().limit(CHUNK_HEADER_SIZE);
            headFile.read(headBuffer);
            headBuffer.flip();
            long headPtr = headBuffer.getLong();
            long tailPtr = headBuffer.getLong();
            return headPtr >= tailPtr;
        }
    }

    public void clear() throws IOException {
        try (FileLock ignored = headFile.lock()) {
            headFile.truncate(0);
        }
    }

    public byte[] peek() throws IOException {
        try (FileLock ignored = headFile.lock()) {
            if (headFile.size() == 0) {
                return null;
            }

            headFile.position(0);
            headBuffer.clear().limit(CHUNK_HEADER_SIZE);
            headFile.read(headBuffer);
            headBuffer.flip();
            long headPtr = headBuffer.getLong();
            long tailPtr = headBuffer.getLong();
            if (headPtr >= tailPtr) {
                // TODO delete empty head
                return null;
            }
            headFile.position(headPtr);

            headBuffer.clear().limit(ENTRY_HEADER_SIZE);
            headFile.read(headBuffer);
            headBuffer.flip();
            final int length = headBuffer.getShort() & 0xFFFF;

            int offset = 0;
            byte[] output = new byte[length];
            while (offset < length) {
                headBuffer.clear();
                headBuffer.limit(Math.min(length - offset, headBuffer.capacity()));
                headFile.read(headBuffer);
                headBuffer.flip();
                headBuffer.get(output, offset, headBuffer.limit());
                offset += headBuffer.limit();
            }

            return output;
        }
    }

    public boolean deqew() throws IOException {
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
    }

    public void enqew(byte[] input) throws IOException {
        try (FileLock ignored = tailFile.lock()) {
            final long headPtr;
            final long tailPtr;
            final int nextRef;
            if (tailFile.size() == 0) {
                headPtr = CHUNK_HEADER_SIZE;
                tailPtr = CHUNK_HEADER_SIZE;
                nextRef = NULL_REF;
                tailFile.truncate(chunkSize);
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
            tailBuffer.putShort((short)(input.length & 0xFFFF));
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
    }

    private static void updateHeader(FileChannel ch, ByteBuffer buf, long headPtr, long tailPtr, int next) throws IOException {
        ch.position(0);
        buf.clear();
        buf.putLong(headPtr);
        buf.putLong(tailPtr);
        buf.putShort((short)(next & 0xFFFF));
        buf.flip();
        ch.write(buf);
    }

    @Override
    public void close() throws IOException {
        FileLock headLock = headFile.lock();
        if (!headLock.channel().equals(tailFile)) {
            tailFile.lock();
            tailFile.close();
        }
        headFile.close();
    }
}
