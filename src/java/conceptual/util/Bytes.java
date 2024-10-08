package conceptual.util;

import org.fressian.FressianReader;
import org.fressian.FressianWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Bytes {

    private Bytes() {}

    // TODO: support Nippy
    static enum EncodingTypes {
        STRING, INT, LONG, FRESSIAN
    }

    public static byte[] stringToBytes(final String s) throws UnsupportedEncodingException {
        byte[] bytes = s.getBytes("UTF-8");
        ByteBuffer bb = ByteBuffer.allocate(bytes.length + 1);
        bb.put((byte) EncodingTypes.STRING.ordinal());
        bb.put(bytes, 0, bytes.length);
        return bb.array();
    }

    public static byte[] objectToBytes(final Object o) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        FressianWriter w = new FressianWriter(os);
        os.write((byte) EncodingTypes.FRESSIAN.ordinal());
        w.writeObject(o);
        return os.toByteArray();
    }

    public static byte[] toBytes(final int v) {
        ByteBuffer bb = ByteBuffer.allocate(5);
        bb.put((byte) EncodingTypes.INT.ordinal());
        bb.putInt(v);
        return bb.array();
    }

    public static byte[] toBytes(final Object o) throws IOException {
        if (o instanceof Integer) return toBytes(((Integer) o).intValue());
        else if (o instanceof String) return stringToBytes((String) o);
        else return objectToBytes(o);
    }

    public static Object fromBytes(final byte[] bytes) throws IOException {
        EncodingTypes type = EncodingTypes.values()[bytes[0]];
        switch (type) {
            case INT: return ByteBuffer.wrap(bytes, 1, 4).getInt();
            case STRING: return new String(bytes, 1, bytes.length - 1);
            case FRESSIAN: {
                return (new FressianReader(new ByteArrayInputStream(bytes, 1, bytes.length - 1))).readObject();
            }
            default: return null;
        }
    }
}
