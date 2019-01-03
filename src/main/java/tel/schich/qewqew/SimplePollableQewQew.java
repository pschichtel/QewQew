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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimplePollableQewQew<E> implements PollableQewQew<E> {

    private final QewQew<E> qew;
    private final Lock lock;
    private final Condition nonEmpty;

    public SimplePollableQewQew(QewQew<E> qew) {
        this(qew, true);
    }

    public SimplePollableQewQew(QewQew<E> qew, boolean fair) {
        this.qew = qew;
        this.lock = new ReentrantLock(fair);
        this.nonEmpty = this.lock.newCondition();
    }

    public static PollableQewQew<byte[]> from(Path queuePath, long chunkSize) throws IOException {
        return new SimplePollableQewQew<>(new SimpleQewQew(queuePath, chunkSize));
    }

    @Override
    public boolean poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (qew.isEmpty() && timeoutNanos > 0) {
                timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
            }
            return !qew.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E peek() throws IOException {
        lock.lock();
        try {
            return qew.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E peek(long timeout, TimeUnit unit) throws IOException, InterruptedException {
        lock.lock();
        try {
            if (poll(timeout, unit)) {
                return qew.peek();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean dequeue() throws IOException {
        lock.lock();
        try {
            qew.dequeue();
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void enqueue(E elem) throws IOException {
        lock.lock();
        try {
            qew.enqueue(elem);
            nonEmpty.signal();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public E dequeue(long timeout, TimeUnit unit) throws IOException, InterruptedException {
        lock.lock();
        try {
            E elem = null;
            if (poll(timeout, unit)) {
                elem = qew.peek();
                if (elem != null) {
                    qew.dequeue();
                }
            }
            return elem;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E dequeueIf(long timeout, TimeUnit unit, DequeueCondition<E> condition) throws IOException, InterruptedException, ExecutionException {
        lock.lock();
        try {
            E elem = peek(timeout, unit);
            try {
                if (elem != null && condition.test(elem)) {
                    qew.dequeue();
                    return elem;
                }
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return qew.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean clear() throws IOException {
        lock.lock();
        try {
            qew.clear();
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            qew.close();
        } finally {
            lock.unlock();
        }
    }
}
