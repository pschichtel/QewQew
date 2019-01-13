package tel.schich.qewqew;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.security.MessageDigest.getInstance;

public class TestHelper {
    public static String hashFile(Path path) throws IOException, NoSuchAlgorithmException {
        final MessageDigest digest = getInstance("MD5");
        try (
            InputStream in = Files.newInputStream(path);
            DigestInputStream digIn = new DigestInputStream(in, digest)
        ) {
            final byte[] buf = new byte[8192];
            while (digIn.read(buf) != -1);
        }

        StringBuilder sb = new StringBuilder(2*digest.getDigestLength());
        for(byte b : digest.digest()){
            sb.append(String.format("%02x", b&0xff));
        }

        return sb.toString();
    }
}
