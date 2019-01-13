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

import static tel.schich.qewqew.SimpleQewQew.CHUNK_HEADER_OFFSET;
import static tel.schich.qewqew.SimpleQewQew.CHUNK_HEADER_SIZE;
import static tel.schich.qewqew.SimpleQewQew.CHUNK_HEAD_PTR_OFFSET;
import static tel.schich.qewqew.SimpleQewQew.CHUNK_NEXT_REF_OFFSET;
import static tel.schich.qewqew.SimpleQewQew.CHUNK_TAIL_PTR_OFFSET;
import static tel.schich.qewqew.SimpleQewQew.ENTRY_HEADER_SIZE;
import static tel.schich.qewqew.SimpleQewQew.NULL_REF;
import static tel.schich.qewqew.SimpleQewQew.getUShort;
import static tel.schich.qewqew.SimpleQewQew.openFile;
import static tel.schich.qewqew.SimpleQewQew.putUShort;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

final class Chunk implements Closeable {

    private final long chunkSize;
    private final Path path;
    final int id;

    private FileChannel file;
    private FileLock lock;
    private MappedByteBuffer map;

    int headPtr;
    int tailPtr;
    int next;

    Chunk(Path path, int id, long chunkSize) {
        this.path = path;
        this.id = id;
        this.chunkSize = chunkSize;
    }

    void open() throws IOException {
        if (this.file != null) {
            return;
        }
        this.file = openFile(path);
        this.lock = file.lock();
        this.map = file.map(FileChannel.MapMode.READ_WRITE, 0, this.chunkSize);
    }

    Chunk init(boolean forceNew) throws IOException {

        this.open();

        if (forceNew) {
            this.file.truncate(chunkSize);
            this.headPtr = CHUNK_HEADER_SIZE;
            this.tailPtr = CHUNK_HEADER_SIZE;
            this.next = NULL_REF;
        } else {
            this.map.position(CHUNK_HEADER_OFFSET);
            this.headPtr = map.getInt(CHUNK_HEAD_PTR_OFFSET);
            this.tailPtr = map.getInt(CHUNK_TAIL_PTR_OFFSET);
            this.next = getUShort(map, CHUNK_NEXT_REF_OFFSET);
        }

        return this;
    }

    void drop() throws IOException {
        close();
        Files.delete(path);
    }

    @Override
    public void close() throws IOException {
        if (this.file != null) {
            this.force();
            this.lock.release();
            this.file.close();
        }
        this.file = null;
        this.lock = null;
        this.map = null;
    }

    void force() {
        this.map.force();
    }

    byte[] peek(byte[] output) {
        this.map.position(this.headPtr + SimpleQewQew.ENTRY_HEADER_SIZE);
        this.map.get(output);
        return output;
    }

    void writeChunkHeader() {
        writeChunkHeadPtr();
        writeChunkTailPtr();
        writeChunkNextRef();
    }

    void writeChunkHeadPtr() {
        this.map.putInt(CHUNK_HEAD_PTR_OFFSET, this.headPtr);
    }

    void writeChunkTailPtr() {
        this.map.putInt(CHUNK_TAIL_PTR_OFFSET, this.tailPtr);
    }

    void writeChunkNextRef() {
        SimpleQewQew.putUShort(this.map, CHUNK_NEXT_REF_OFFSET, this.next);
    }

    void putPayload(byte[] payload, int offset, int length) {
        putUShort(this.map, this.tailPtr, length);
        this.map.position(this.tailPtr + ENTRY_HEADER_SIZE);
        this.map.put(payload, offset, length);
    }

    int peekLength() {
        return getUShort(this.map, this.headPtr);
    }


}
