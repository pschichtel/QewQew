import java.io.IOException;
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

    @Override
    public boolean poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (qew.isEmpty() && timeoutNanos > 0) {
                timeoutNanos = nonEmpty.awaitNanos(timeoutNanos);
            }
            return qew.isEmpty();
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
