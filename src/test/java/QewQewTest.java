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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QewQewTest {

    static final Path HEAD_PATH = Paths.get("qew-head");

    public static final int BLOCK_SIZE = 4096;
    public static final int CHUNK_SIZE = 1024 * 1024;

    @Test
    void testQueue() throws IOException {
        try (QewQew q = new QewQew(HEAD_PATH, BLOCK_SIZE, CHUNK_SIZE, true)) {
            q.clear();
            assertTrue(q.isEmpty());

            q.enqew(buf('a', 'b', 'c'));
            assertTrue(!q.isEmpty());

            String s = new String(q.peek());
            assertEquals("abc", s);

            assertTrue(q.deqew());
            assertTrue(q.isEmpty());
        }
    }

    @Test
    void testManyWrites() throws IOException {
        Random r = new Random(1);
        try (QewQew q = new QewQew(HEAD_PATH, BLOCK_SIZE, CHUNK_SIZE, true)) {
            q.clear();
            for (int i = 0; i < 1000; ++i) {
                q.enqew(buf(r.nextInt(((short) -1) & 0xFFFF)));
            }
        }
    }

    static byte[] buf(int... data) {
        byte[] out = new byte[data.length];
        for (int i = 0; i < data.length; i++) {
            out[i] = (byte) data[i];
        }
        return out;
    }
}