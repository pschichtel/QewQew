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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

final class Chunk implements Closeable {
    final Path path;
    final FileChannel file;
    int headPtr;
    int tailPtr;
    final FileLock lock;
    final int id;
    int next;
    final MappedByteBuffer map;

    Chunk(Path path, FileChannel file, FileLock lock, MappedByteBuffer map, int headPtr, int tailPtr, int id, int next) {
        this.path = path;
        this.file = file;
        this.lock = lock;
        this.id = id;
        this.headPtr = headPtr;
        this.tailPtr = tailPtr;
        this.next = next;
        this.map = map;
    }

    void drop() throws IOException {
        map.force();
        close();
        Files.delete(path);
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
