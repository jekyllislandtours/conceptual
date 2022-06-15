package conceptual.util;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.iq80.snappy.SnappyFramedInputStream;
import org.iq80.snappy.SnappyFramedOutputStream;

public class ZipTools {

    public static InputStream getCompressedInputStream(InputStream is, String filename) throws IOException {
        if (filename.endsWith(".gz"))
            return new GZIPInputStream(is);
        if (filename.endsWith(".bz2"))
            return new BZip2CompressorInputStream(is);
        if (filename.endsWith(".sz"))
            return new SnappyFramedInputStream(is, false);
        else return is;
    }

    public static OutputStream getCompressedOutputStream(OutputStream os, String filename) throws IOException {
        if (filename.endsWith(".gz"))
            return new GZIPOutputStream(os, 0x100_000, true);
        if (filename.endsWith(".bz2"))
            return new BZip2CompressorOutputStream(os);
        if (filename.endsWith(".sz"))
            return new SnappyFramedOutputStream(os);
        else return os;
    }
}
