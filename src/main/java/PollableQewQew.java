import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface PollableQewQew<E> extends QewQew<E> {
    boolean poll(long timeout, TimeUnit unit) throws InterruptedException;
    E peek(long timeout, TimeUnit unit) throws IOException, InterruptedException;
}
