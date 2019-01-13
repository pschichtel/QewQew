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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static tel.schich.qewqew.TestHelper.hashFile;

class SimpleQewQewTest {

    public static final int CHUNK_SIZE = 1024;

    Path randomHeadPath() {
        return Paths.get("/tmp/qew-" + (new Random().nextInt()) + ".qew");
    }

    @Test
    void testQueue() throws IOException {
        final Path headPath = randomHeadPath();
        try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            q.clear();
            assertTrue(q.isEmpty());

            q.enqueue(buf('a', 'b', 'c'));
            assertFalse(q.isEmpty());

            String s = new String(q.peek());
            assertEquals("abc", s);

            assertTrue(q.dequeue());
            assertTrue(q.isEmpty());
        }
    }

    /**
     * This test verifies the chunk size upper bound.
     * The chunk needs enough space fit not just the payload, but its length as well.
     */
    @Test
    void testChunkSizeEdge() throws IOException {
        final Path headPath = randomHeadPath();
        final byte[] payload = {1, 2, 3};
        // chunk fits its header, 1 entry header (length) and 2 same-size payloads
        final int chunkSize = SimpleQewQew.CHUNK_HEADER_SIZE + SimpleQewQew.ENTRY_HEADER_SIZE + 2 * payload.length;

        try (SimpleQewQew q = new SimpleQewQew(headPath, chunkSize)) {
            q.clear();
            assertTrue(q.isEmpty());

            // payload fits once into a chunk
            q.enqueue(payload);
            assertFalse(q.isEmpty());
            assertEquals(1, q.countChunks());

            // but not twice
            q.enqueue(payload);
            assertEquals(2, q.countChunks());

            assertTrue(q.dequeue());
            assertFalse(q.isEmpty());
            assertEquals(1, q.countChunks());

            q.clear();
        }
    }

    @Test
    void testMultipleOpen() throws IOException {
        final Path headPath = randomHeadPath();
        try (SimpleQewQew ignored = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            assertThrows(QewAlreadyOpenException.class, () -> new SimpleQewQew(headPath, CHUNK_SIZE));
        }
    }

    @Test
    void testManyWrites() throws IOException {
        final Path headPath = randomHeadPath();
        final Random r = new Random(1);
        final Queue<byte[]> expectedBuffers = new ArrayDeque<>();
        try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            q.clear();
            for (int i = 0; i < 1000; ++i) {
                byte[] buf = buf(r.nextInt(((short) -1) & 0xFFFF), r.nextInt(((short) -1) & 0xFFFF));
                expectedBuffers.add(buf);
                q.enqueue(buf);
            }
            assertFalse(q.isEmpty());
        }


        try (SimpleQewQew actualBuffers = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            while (!actualBuffers.isEmpty()) {
                byte[] expected = expectedBuffers.remove();
                byte[] actual = actualBuffers.peek();
                assertTrue(actualBuffers.dequeue());
                assertArrayEquals(expected, actual);
            }

            assertTrue(actualBuffers.isEmpty());
            assertTrue(expectedBuffers.isEmpty());
            assertFalse(actualBuffers.clear());
        }
    }

    @Test
    void testQueueHeadUpdate() throws IOException, NoSuchAlgorithmException {
        final Path headPath = randomHeadPath();
        final Random r = new Random(1);
        try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            final int bufSize = (int) q.getMaxElementSize();
            q.enqueue(random(r, bufSize));
            q.enqueue(random(r, bufSize));
        }

        final String initialHash = hashFile(headPath);
        try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            q.dequeue();
        }
        assertNotEquals(initialHash, hashFile(headPath));

        try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
            assertTrue(q.clear());
        }
    }

    @Test
    void testQueueIsTooBig() {
        assertThrows(BufferOverflowException.class, () -> {
            final Path headPath = randomHeadPath();
            final Random r = new Random(1);
            try (SimpleQewQew q = new SimpleQewQew(headPath, CHUNK_SIZE)) {
                final int bufSize = (int) q.getMaxElementSize();
                q.enqueue(random(r, bufSize + 1));
            }
        });
    }

    static byte[] buf(int... data) {
        byte[] out = new byte[data.length];
        for (int i = 0; i < data.length; i++) {
            out[i] = (byte) data[i];
        }
        return out;
    }

    static byte[] random(Random r, int size) {
        byte[] buf = new byte[size];
        r.nextBytes(buf);
        return buf;
    }
}
