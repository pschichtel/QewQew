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