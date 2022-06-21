package conceptual.core;

import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.MapEntry;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;

import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentList;
import clojure.lang.PersistentQueue;
import clojure.lang.PersistentStructMap;
import clojure.lang.PersistentTreeMap;
import clojure.lang.PersistentTreeSet;
import clojure.lang.PersistentVector;
import clojure.java.api.Clojure;

import conceptual.util.IntArrayPool;

import java.io.*;

import java.time.Instant;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DBTranscoder {

    public static final IFn clojureReadString = Clojure.var("clojure.edn", "read-string");
    public static final IFn clojureStr = Clojure.var("clojure.core", "str");

    public static final IFn nippyFreeze = Clojure.var("taoensso.nippy", "freeze");
    public static final IFn nippyThaw = Clojure.var("taoensso.nippy", "thaw");

    /** Constant <code>VERSION=1</code> */
    public static final int VERSION = 1;

    // NOTE: enums do not version well
    public static final int NULL = 0;
    public static final int STRING = 1;
    public static final int KEYWORD = 2;
    public static final int INT = 3;
    public static final int LONG = 4;
    public static final int FLOAT = 5;
    public static final int DOUBLE = 6;
    public static final int CHARACTER = 7;
    public static final int BOOLEAN = 8;
    public static final int DATE = 9;
    public static final int INSTANT = 10;
    public static final int CLASS = 11;
    public static final int STRING_ARRAY = 12;
    public static final int KEYWORD_ARRAY = 13;
    public static final int INT_ARRAY = 14;
    public static final int LONG_ARRAY = 15;
    public static final int FLOAT_ARRAY = 16;
    public static final int DOUBLE_ARRAY = 17;
    public static final int BOOLEAN_ARRAY = 18;
    public static final int CHARACTER_ARRAY = 19;
    public static final int DATE_ARRAY = 20;
    public static final int INSTANT_ARRAY = 21;
    public static final int EDN = 22;

    public static String nsname(Keyword k) {
        if (k.getNamespace() != null) {
            return k.getNamespace() + "/" + k.getName();
        } else {
            return k.getName();
        }
    }

    public static void encode(DataOutputStream dos, RDB db) throws IOException {
        if (db == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            dos.writeInt(VERSION);
            dos.writeUTF(nsname(db.getIdentity()));
            dos.writeInt(db.getMaxId());
            encodeKeyIdIndex(dos, db.keyIdIndex);
            encodeKeyIndex(dos, db);
            encodeValIndex(dos, db);
        }
    }

    public static RDB decodeRDB(DataInputStream dis) throws IOException {
        RDB result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            Keyword identity;
            int maxId;
            IPersistentMap keyIdIndex;
            int[][] keyIndex;
            Object[][] valIndex;
            int version = dis.readInt();
            if (version != VERSION) {
                throw new RuntimeException("Version " + version + " not supported by DBTranscoder.");
            }
            identity = Keyword.intern(dis.readUTF());
            maxId = dis.readInt();
            keyIdIndex = decodeKeyIdIndex(dis);
            keyIndex = decodeKeyIndex(dis);
            valIndex = decodeValIndex(dis);
            RDB.C[] cs = new RDB.C[keyIndex.length];
            System.out.println("\nconstructing core index: ");
            double last = 0.0;
            double current;
            double lengthD = (double) maxId;
            for (int i = 0; i <= maxId; i++) {
                cs[i] = new RDB.C(keyIndex[i], valIndex[i]);
                current = Math.round(((double) i/ lengthD) * 100.0d);
                if (current != last) {
                    last = current;
                    System.out.printf("\r    %3.0f%% complete.", current);
                }
            }
            System.out.println();
            result = new RDB(identity, keyIdIndex, cs, maxId, new IntArrayPool());
        }
        return result;
    }

    public static void encodeKeyIdIndex(final DataOutputStream dos, final IPersistentMap map) throws IOException {
        if (map == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            int count = map.count();
            dos.writeInt(count);
            for (Iterator iter = map.iterator(); iter.hasNext(); ) {
                MapEntry entry = (MapEntry) iter.next();
                Keyword keyword = (Keyword) entry.key();
                String ns = keyword.getNamespace();
                if (ns != null) {
                    dos.writeUTF(ns + "/" + keyword.getName());
                } else {
                    dos.writeUTF(keyword.getName());
                }
                dos.writeInt((Integer) entry.val());
            }
        }
    }

    public static IPersistentMap decodeKeyIdIndex(final DataInputStream dis) throws IOException {
        IPersistentMap result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int count = dis.readInt();
            Map temp = new HashMap(count);
            for (int i=0; i < count; i++) {
                String nsname = dis.readUTF();
                Keyword key = Keyword.intern(nsname);
                int id = dis.readInt();
                temp.put(key, id);
            }
            result = PersistentHashMap.create(temp);
        }
        return result;
    }

    public static void encodeKeyIndex(final DataOutputStream dos, final RDB db) throws IOException {
        if (db == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            RDB.C[] cs = db.cs;
            dos.writeInt(db.maxId + 1);
            for (int i=0; i <= db.maxId; i++) {
                encodeKeys(dos, cs[i].ks);
            }
        }
    }

    public static byte[] encodeKeys(final int[] keys) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        encodeKeys(dos, keys);
        return bos.toByteArray();
    }

    public static void encodeKeys(final DataOutputStream dos, final int[] keys) throws IOException {
        if (keys == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            dos.writeInt(keys.length);
            for (int i=0; i < keys.length; i++) {
                dos.writeInt(keys[i]);
            }
        }
    }

    public static byte[] encodeKeys(final byte[][] keys) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        encodeKeys(dos, keys);
        return bos.toByteArray();
    }

    public static void encodeKeys(final DataOutputStream dos, final byte[][] keys) throws IOException {
        if (keys == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            dos.writeInt(keys.length);
            for (int i=0; i < keys.length; i++) {
                dos.writeInt(keys[i].length);
                dos.write(keys[i]);
            }
        }
    }

    public static int[][] decodeKeyIndex(final DataInputStream dis) throws IOException {
        int[][] result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int length = dis.readInt();
            result = new int[length][];
            System.out.println("\ndecoding key index: ");
            double last = 0.0;
            double current;
            double lengthD = (double) length;
            for (int i=0; i < length; i++) {
                result[i] = decodeKeys(dis);
                current = Math.round(((double) i/ lengthD) * 100.0d);
                if (current != last) {
                    last = current;
                    System.out.printf("\r    %3.0f%% loaded.", current);
                }
            }
            System.out.println();
        }
        return result;
    }

    public static int[] decodeKeys(byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            return decodeKeys(dis);
        }
        return null;
    }

    public static int[] decodeKeys(final DataInputStream dis) throws IOException {
        int[] result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int length = dis.readInt();
            result = new int[length];
            for (int i=0; i < length; i++) {
                result[i] = dis.readInt();
            }
        }
        return result;
    }


    public static byte[][] decodeByteKeys(byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            return decodeByteKeys(dis);
        }
        return null;
    }

    public static byte[][] decodeByteKeys(final DataInputStream dis) throws IOException {
        byte[][] result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int length = dis.readInt();
            result = new byte[length][];
            byte[] temp = null;
            for (int i=0; i < length; i++) {
                temp = new byte[dis.readInt()];
                dis.readFully(temp);
                result[i] = temp;
            }
        }
        return result;
    }

    public static void encodeValIndex(final DataOutputStream dos, final RDB db) throws IOException {
        if (db == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            dos.writeInt(db.maxId + 1);
            RDB.C[] cs = db.cs;
            for (int i=0; i <= db.maxId; i++) {
                encodeVals(dos, cs[i].vs);
            }
        }
    }

    public static byte[] encodeVals(final Object[] vals) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        encodeVals(dos, vals);
        return bos.toByteArray();
    }

    public static void encodeVals(final DataOutputStream dos, final Object[] vals) throws IOException {
        if (vals == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(1);
            dos.writeInt(vals.length);
            for (int i=0; i < vals.length; i++) {
                encodeVal(dos, vals[i]);
            }
        }
    }

    public static Object[][] decodeValIndex(final DataInputStream dis) throws IOException {
        Object[][] result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int length = dis.readInt();
            result = new Object[length][];
            System.out.println("\ndecoding val index: ");
            double last = 0.0;
            double current;
            double lengthD = (double) length;
            for (int i=0; i < length; i++) {
                result[i] = decodeVals(dis);
                current = Math.round(((double) i/ lengthD) * 100.0d);
                if (current != last) {
                    last = current;
                    System.out.printf("\r    %3.0f%% loaded.", current);
                }
            }
            System.out.println();
        }
        return result;
    }

    public static Object[] decodeVals(byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            BufferedInputStream bufis = new BufferedInputStream(bis, 8 * 1024);
            DataInputStream dis = new DataInputStream(bufis);
            return decodeVals(dis);
        }
        return null;
    }

    public static Object[] decodeVals(final DataInputStream dis) throws IOException {
        Object[] result = null;
        int oneIfNotNull = dis.readInt();
        if (oneIfNotNull == 1) {
            int length = dis.readInt();
            result = new Object[length];
            for (int i=0; i < length; i++) {
                result[i] = decodeVal(dis);
            }
        }
        return result;
    }

    public static byte[] encodeVal(final Object val) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedOutputStream bufos = new BufferedOutputStream(bos, 8 * 1024);
        DataOutputStream dos = new DataOutputStream(bos);
        encodeVal(dos, val);
        return bos.toByteArray();
    }

    // TODO: missing DATETIME_ARRAY, INSTANT_ARRAY
    public static void encodeVal(final DataOutputStream dos, final Object val) throws IOException {
        if (val != null) {
            Class<?> clazz = val.getClass();
            if (clazz == String.class) {
                dos.writeInt(STRING);
                dos.writeUTF((String) val);
            } else if (clazz == Keyword.class) { // relation
                dos.writeInt(KEYWORD);
                Keyword key = ((Keyword) val);
                String ns = key.getNamespace();
                if (ns != null) {
                    dos.writeUTF(ns + "/" + key.getName());
                } else {
                    dos.writeUTF(key.getName());
                }
            } else if (clazz == Integer.class) { // relation
                dos.writeInt(INT);
                dos.writeInt((Integer) val);
            } else if (clazz == Double.class) {
                dos.writeInt(DOUBLE);
                dos.writeDouble((Double) val);
            } else if (clazz == int[].class) { // relation
                dos.writeInt(INT_ARRAY);
                int[] data = (int[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeInt(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == Long.class) {
                dos.writeInt(LONG);
                dos.writeLong((Long) val);
            } else if (clazz == Float.class) {
                dos.writeInt(FLOAT);
                dos.writeFloat((Float) val);
            } else if (clazz == Boolean.class) {
                dos.writeInt(BOOLEAN);
                dos.writeBoolean((Boolean) val);
            } else if (clazz == Character.class) {
                dos.writeInt(CHARACTER);
                dos.writeChar((Character) val);
            } else if (clazz == Date.class) { // purposefully normalizing on joda time
                dos.writeInt(INSTANT);
                dos.writeLong(((Date) val).getTime());
            } else if (clazz == Instant.class) {
                dos.writeInt(INSTANT);
                dos.writeLong(((Instant) val).toEpochMilli());
            } else if (clazz == java.sql.Timestamp.class) { // purposefully normalizing on joda time
                dos.writeInt(INSTANT);
                dos.writeLong(((Date) val).getTime());
            } else if (clazz == java.lang.Class.class) {
                dos.writeInt(CLASS);
                if (val == java.sql.Timestamp.class ||
                    val == java.util.Date.class) {
                    dos.writeUTF(java.time.Instant.class.getName());
                } else {
                    dos.writeUTF(((Class) val).getName());
                }
            } else if (clazz == String[].class) {
                dos.writeInt(STRING);
                String[] data = (String[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeUTF(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == double[].class) {
                dos.writeInt(DOUBLE_ARRAY);
                double[] data = (double[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeDouble(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == boolean[].class) {
                dos.writeInt(BOOLEAN_ARRAY);
                boolean[] data = (boolean[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeBoolean(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == long[].class) {
                dos.writeInt(LONG_ARRAY);
                long[] data = (long[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeLong(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == char[].class) {
                dos.writeInt(CHARACTER_ARRAY);
                char[] data = (char[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeChar(data[j]);
                    }
                } else {
                    dos.writeInt(0);
                }
            }  else if (clazz == Instant[].class) {
                dos.writeInt(INSTANT_ARRAY);
                Instant[] data = (Instant[]) val;
                if (data != null) {
                    dos.writeInt(data.length);
                    for (int j = 0; j < data.length; j++) {
                        dos.writeLong(data[j].toEpochMilli());
                    }
                } else {
                    dos.writeInt(0);
                }
            } else if (clazz == PersistentArrayMap.class ||
                       clazz == PersistentHashMap.class ||
                       clazz == PersistentHashSet.class ||
                       clazz == PersistentList.class ||
                       clazz == PersistentQueue.class ||
                       clazz == PersistentStructMap.class ||
                       clazz == PersistentTreeMap.class ||
                       clazz == PersistentTreeSet.class ||
                       clazz == PersistentVector.class) {
                encodeEDN(dos, val);
            } else {
                throw new IOException("Could not encode unknown type: " + clazz.getName() + ", val: " + val);
            }
        } else {
            dos.writeInt(NULL);
        }
    }

    public static final int MAX_UTF_LENGTH = 65535;

    public static String[] chunkString(final String s, final int length) {
        int n = 0;
        String[] result = null;
        try {
            n = ((Double) Math.ceil((double) s.length() / (double) length)).intValue();
            // System.out.println("n: " + n);
            result = new String[n];
            for (int i=0, j=0; i < s.length(); i += length, j++) {
                // System.out.println(i + "," + s.length() + "," + (i < s.length()));
                // System.out.println("[" + i + "," + j + "," +
                //                    (i + length) + "," + s.length() + "," +
                //                    Math.min(i + length, s.length()) + "]");
                String temp = s.substring(i, Math.min(i + length, s.length()));
                // System.out.println(temp);
                result[j] = temp;
            }
        } catch (Exception e) {
            System.out.println("ERROR Chunking:" + n + ":\n" + s);
        }
        return result;
    }

    public static void encodeEDN(final DataOutputStream dos, final Object val) throws IOException {
        dos.writeInt(EDN);
        String data = (String) clojureStr.invoke(val);
        String[] chunks = chunkString(data, MAX_UTF_LENGTH);
        if (chunks != null) {
            dos.writeInt(chunks.length);
            for (int i=0; i < chunks.length; i++) {
                dos.writeUTF(chunks[i]);
            }
        } else {
            dos.writeInt(0);
        }
    }

    public static void encodeEDNString(final DataOutputStream dos, final Object val) throws IOException {
        dos.writeInt(EDN);
        String data = (String) clojureStr.invoke(val);
        String[] chunks = chunkString(data, MAX_UTF_LENGTH);
        if (chunks != null) {
            dos.writeInt(chunks.length);
            for (int i=0; i < chunks.length; i++) {
                dos.writeUTF(chunks[i]);
            }
        } else {
            dos.writeInt(0);
        }
    }

    public static Object decodeEDN(final DataInputStream dis) throws IOException {
        Object result = null;
        int length = dis.readInt();
        StringBuilder builder = new StringBuilder();
        for (int i=0; i < length; i++) {
            builder.append(dis.readUTF());
        }
        String edn = builder.toString();
        try {
            if (edn != null && !edn.equals(""))
                result = clojureReadString.invoke(builder.toString());
        } catch (Exception e) {
            System.out.println("ERROR reading EDN:" + e.getMessage() + "\n\n" +
                               builder.toString());
        }
        return result;
    }

    public static Object decodeEDNString(final DataInputStream dis) throws IOException {
        Object result = null;
        int length = dis.readInt();
        StringBuilder builder = new StringBuilder();
        for (int i=0; i < length; i++) {
            builder.append(dis.readUTF());
        }
        String edn = builder.toString();
        try {
            if (edn != null && !edn.equals(""))
            result = clojureReadString.invoke(builder.toString());
        } catch (Exception e) {
            System.out.println("ERROR reading EDN:" + e.getMessage() + "\n\n" +
                               builder.toString());
        }
        return result;
    }

    public static Object decodeVal(byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            BufferedInputStream bufis = new BufferedInputStream(bis, 8 * 1024);
            DataInputStream dis = new DataInputStream(bufis);
            return decodeVal(dis);
        }
        return null;
    }

    public static Object decodeVal(final DataInputStream dis) throws IOException {
        int type = dis.readInt();
        switch (type) {
            case INT: {
                return dis.readInt();
            }
            case INT_ARRAY: {
                int length = dis.readInt();
                int[] result = new int[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readInt();
                }
                return result;
            }
            case STRING: {
                return dis.readUTF();
            }
            case INSTANT: {
                return Instant.ofEpochMilli(dis.readLong());
            }
            case DOUBLE: {
                return dis.readDouble();
            }
            case KEYWORD: {
                return Keyword.intern(dis.readUTF());
            }
            case LONG: {
                return dis.readLong();
            }
            case FLOAT: {
                return dis.readFloat();
            }
            case CHARACTER: {
                return dis.readChar();
            }
            case BOOLEAN: {
                return dis.readBoolean();
            }
            case DATE: {
                return Instant.ofEpochMilli(dis.readLong());
            }
            case NULL: {
                return null;
            }
            case CLASS: {
                try {
                    return Class.forName(dis.readUTF());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    throw new IOException(e.getMessage());
                }
            }
            case STRING_ARRAY: {
                int length = dis.readInt();
                String[] result = new String[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readUTF();
                }
                return result;
            }
            case KEYWORD_ARRAY: {
                int length = dis.readInt();
                Keyword[] result = new Keyword[length];
                for (int i=0; i < length; i++) {
                    result[i] = Keyword.intern(dis.readUTF());
                }
                return result;
            }
            case LONG_ARRAY: {
                int length = dis.readInt();
                long[] result = new long[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readLong();
                }
                return result;
            }
            case DOUBLE_ARRAY: {
                int length = dis.readInt();
                double[] result = new double[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readDouble();
                }
                return result;
            }
            case FLOAT_ARRAY: {
                int length = dis.readInt();
                float[] result = new float[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readFloat();
                }
                return result;
            }
            case CHARACTER_ARRAY: {
                int length = dis.readInt();
                char[] result = new char[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readChar();
                }
                return result;
            }
            case BOOLEAN_ARRAY: {
                int length = dis.readInt();
                boolean[] result = new boolean[length];
                for (int i=0; i < length; i++) {
                    result[i] = dis.readBoolean();
                }
                return result;
            }
                //case DATE_ARRAY: {
                //int length = dis.readInt();
                //Date[] result = new Date[length];
                //for (int i=0; i < length; i++) {
                //    result[i] = new Date(dis.readLong());
                //}
                //return result;
                //}
            case INSTANT_ARRAY: {
                int length = dis.readInt();
                Instant[] result = new Instant[length];
                for (int i=0; i < length; i++) {
                    result[i] = Instant.ofEpochMilli(dis.readLong());
                }
                return result;
            }
            case EDN: {
                return decodeEDN(dis);
            }
            default: throw new RuntimeException("Error decoding value.");
        }
    }

    public static void main(String[] args) throws IOException {
        byte[] bytes = DBTranscoder.encodeKeys(new int[] {1, 2, 3});
        int[] keys = DBTranscoder.decodeKeys(bytes);
        System.out.println(keys.length + ", " + keys[1]);

        bytes = DBTranscoder.encodeVals(new Object[] {Keyword.intern("hello/world"), "dude", 1});
        Object[] vals = DBTranscoder.decodeVals(bytes);
        System.out.println(vals.length + ", " + vals[0]);

        bytes = DBTranscoder.encodeVal(Keyword.intern("hello/world"));
        Keyword val = (Keyword) DBTranscoder.decodeVal(bytes);
        System.out.println(val);

        String[] chunks = chunkString("a", 3);
        for (int i=0; i < chunks.length; i++) {
            System.out.println(chunks[i]);
        }
    }
}
