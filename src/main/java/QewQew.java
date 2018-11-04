import java.io.Closeable;
import java.io.IOException;

public interface QewQew<E> extends Closeable {
    E peek() throws IOException;
    boolean dequeue() throws IOException;
    void enqueue(E elem) throws IOException;
    boolean isEmpty();
    boolean clear() throws IOException;
}
