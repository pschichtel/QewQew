import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;

public class QewAlreadyOpenException extends IOException {
    public QewAlreadyOpenException() {
        super();
    }
    public QewAlreadyOpenException(Throwable cause) {
        super(cause);
    }
}
