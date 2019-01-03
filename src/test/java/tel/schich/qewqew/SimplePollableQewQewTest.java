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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static tel.schich.qewqew.SimpleQewQewTest.CHUNK_SIZE;

class SimplePollableQewQewTest {

    private interface QueueTest<T> {
        void test(SimplePollableQewQew<T> q) throws Exception;
    }

    private static void withQueue(String file, QueueTest<byte[]> f) throws Exception {
        Path path = Paths.get(file);
        try (SimpleQewQew q = new SimpleQewQew(path, CHUNK_SIZE)) {
            SimplePollableQewQew<byte[]> pollable = new SimplePollableQewQew<>(q);
            f.test(pollable);
        }
    }

    @Test
    void poll() throws Exception {
        withQueue("poll-test.qew", q -> {
            long start = System.currentTimeMillis();
            boolean result = q.poll(1, SECONDS);
            long delta = (System.currentTimeMillis() - start) / 1000;
            assertEquals(1, delta);
            assertFalse(result);

            byte[] input = {1, 2, 3};
            q.enqueue(input);
            assertTimeout(Duration.ofMillis(100), () -> {
                assertTrue(q.poll(1, SECONDS));
            });

            q.clear();
        });
    }

    @Test
    void peek() throws Exception {
        withQueue("peek-test.qew", q -> {
            long start = System.currentTimeMillis();
            byte[] nullResult = q.peek(1, SECONDS);
            long delta = (System.currentTimeMillis() - start) / 1000;
            assertEquals(1, delta);
            assertNull(nullResult);

            byte[] input = {1, 2, 3};
            q.enqueue(input);
            assertTimeout(Duration.ofMillis(100), () -> {
                byte[] nonNullResult = q.peek(1, SECONDS);
                assertArrayEquals(input, nonNullResult);
            });

            q.clear();
        });
    }

    @Test
    void dequeue() throws Exception {
        withQueue("dequeue-test.qew", q -> {
            long start = System.currentTimeMillis();
            byte[] nullResult = q.dequeue(1, SECONDS);
            long delta = (System.currentTimeMillis() - start) / 1000;
            assertEquals(1, delta);
            assertNull(nullResult);

            byte[] input = {1, 2, 3};
            q.enqueue(input);
            assertTimeout(Duration.ofMillis(100), () -> {
                byte[] nonNullResult = q.dequeue(1, SECONDS);
                assertArrayEquals(input, nonNullResult);
            });

            q.clear();
        });
    }

    @Test
    void dequeueIf() throws Exception {
        withQueue("dequeue-test.qew", q -> {
            PollableQewQew.DequeueCondition<byte[]> TRUE = a -> true;
            PollableQewQew.DequeueCondition<byte[]> FALSE = a -> false;


            byte[] input = {1, 2, 3};
            q.enqueue(input);
            assertNull(q.dequeueIf(1, SECONDS, FALSE));
            assertArrayEquals(input, q.dequeueIf(1, SECONDS, TRUE));

            q.clear();
        });
    }
}