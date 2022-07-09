package conceptual.core;

import clojure.java.api.Clojure;

import clojure.lang.IFn;
import clojure.lang.Atom;
import clojure.lang.Symbol;
import clojure.lang.Keyword;
import org.cojen.tupl.Database;
import org.cojen.tupl.Index;

/**
 * This class provides the Clojure Java interop. This is really only
 * needed when your working from a non-clojure VM language.
 */
public class InteropFacade {

    static IFn deref = Clojure.var("clojure.core", "deref");
    static IFn require = Clojure.var("clojure.core", "require");

    /**
     * Returns the Concept DB. Try to call just once per request.
     *
     * @return DB the concept DB.
     */
    public static DB getDB() {
        return (DB) derefAtom("*db*");
    }

    /**
     * Returns the Database associated with the given key.
     * If it does not already exist, it is opened.
     * @param key the key associated with the db.
     * @return Database the Database associated with the key.
     */
    public static Database getTuplDatabase(Keyword key) {
        IFn fn = Clojure.var("conceptual.tupl", "db");
        return (Database) fn.invoke(key);
    }

    /**
     * Returns the Index associated with the given key.
     * If it does not already exist, it is opened.
     * @param key the key associated with the db.
     * @return Index the Index associated with the key.
     */
    public static Index getTuplIndex(Keyword key) {
        IFn fn = Clojure.var("conceptual.tupl", "index");
        return (Index) fn.invoke(key);
    }

    /**
     * Returns the Index associated with the given key.
     * If it does not already exist, it is opened.
     * #param database the database to lookup the index in.
     * @param key the key assocated with the index.
     * @return Index the Index associated with the key.
     */
    public static Index getTuplIndex(Database database, Keyword key) {
        IFn fn = Clojure.var("conceptual.tupl", "index");
        return (Index) fn.invoke(database, key);
    }

    /**
     * Encode an object into bytes ready for Tupl.
     * @param o the object to encode.
     * @return byte[] the object encoded into bytes.
     */
    public static byte[] encodeBytes(Object o) {
        IFn fn = Clojure.var("conceptual.arrays", "ensure-bytes");
        return (byte[]) fn.invoke(o);
    }

    /**
     * Decode a result stored in a Tupl Database.
     * @param bytes the bytes to decode into an Object.
     * @return Object the decoded object.
     */
    public static Object decodeBytes(byte[] bytes) {
        IFn fn = Clojure.var("conceptual.arrays", "->bytes");
        return fn.invoke(bytes);
    }

    /**
     * Loads an object from a Tupl Index in the default database given a key.
     * @param idx the index to use.
     * @param key the key associated with the value.
     * @return the Object associated with the key for the given index in the default db.
     */
    public static Object load(Index idx, Object key) {
        IFn fn = Clojure.var("conceptual.tupl", "seek");
        return fn.invoke(idx, key);
    }

    /**
     * Loads an object from a Tupl Index in the given database given a key.
     * @param db the database to use.
     * @param idx the index to use.
     * @param key the key associated with the value.
     * @return the Object associated with the key for the given index in the given db.
     */
    public static Object load(Database db, Index idx, Object key) {
        IFn fn = Clojure.var("conceptual.tupl", "seek");
        return fn.invoke(db, idx, key);
    }

    /**
     * Loads an object from a Tupl Index in the default database given a key.
     * @param idxKey the index to use.
     * @param key the key associated with the value.
     * @return the Object associated with the key for the given index in the default db.
     */
    public static Object load(Keyword idxKey, Object key) {
        IFn fn = Clojure.var("conceptual.tupl", "seek");
        return fn.invoke(idxKey, key);
    }

    /**
     * Loads an object from a Tupl Index in the given database given a key.
     * @param dbKey the database to use.
     * @param idxKey the index to use.
     * @param key the key associated with the value.
     * @return the Object associated with the key for the given index in the given db.
     */
    public static Object load(Keyword dbKey, Keyword idxKey, Object key) {
        IFn fn = Clojure.var("conceptual.tupl", "seek");
        return fn.invoke(dbKey, idxKey, key);
    }

    /**
     * Returns the set of concepts ids who's unique keys contain the given
     * String prefix.
     *
     * @param prefix a prefix to search for.
     * @return int[] the set of concept's matched on.
     */
    public static int[] getByPrefix(String prefix) {
        IFn fn = Clojure.var("conceptual.word-fixes", "ids-by-prefix");
        return (int[]) fn.invoke(prefix);
    }

    /**
     * Returns the set of concepts ids who's unique :-key Keyword contains the given
     * String infix substring.
     *
     * @param infix a infix to search for.
     * @return int[] the set of concept's matched on.
     */
    public static int[] getByInfix(String infix) {
        IFn fn = Clojure.var("conceptual.word-fixes", "ids-by-infix");
        return (int[]) fn.invoke(infix);
    }

    /**
     * Returns a set of concept ids who's unique :-key Keyword contains the given
     * String prefix phrase. This method is used by real-time auto-complete like
     * searches to narrow down the set of keys.
     *
     * Phrase can be delimited by the following:
     *   - whitespace
     *   - . period
     *   - - dash
     *   - _ underscore
     *   - / forward slash
     *
     * @param prefixPhrase
     *         the prefixPhrase approximation to narrow down a set of keys.
     * @return int[]
     *         an array of ids that match the prefixPhrase approximation.
     */
    public static int[] getByPrefixPhrase(String prefixPhrase) {
        IFn fn = Clojure.var("conceptual.word-fixes", "ids-by-prefix-phrase");
        return (int[]) fn.invoke(prefixPhrase);
    }

    /**
     * Returns a set of ids for a prefixPhrase best guess.
     * This method is used by real-time searches to narrow
     * down the set of keys.
     *
     * Phrase can be delimited by the following:
     *   - whitespace
     *   - . period
     *   - - dash
     *   - _ underscore
     *   - / forward slash
     *
     * @param infixPhrase
     *         the prefixPhrase approximation to narrow down a set of keys.
     * @return int[]
     *         an array of ids that match the prefixPhrase approximation.
     */
    public static int[] getByInfixPhrase(String infixPhrase) {
        IFn fn = Clojure.var("conceptual.word-fixes", "ids-by-infix-phrase");
        return (int[]) fn.invoke(infixPhrase);
    }

    /**
     * Requires a given clojure package. This can be used to
     * run a clojure file like a script.
     *
     * @param scriptPackage
     */
    public static void run(String scriptPackage) {
        Symbol symbol = (Symbol) Clojure.read(scriptPackage);
        if (symbol != null) {
            require.invoke(symbol);
        } else {
            throw new RuntimeException("Error loading clojure script: " + scriptPackage);
        }
    }

    public static void buildIndexes() {
        IFn fn = Clojure.var("conceptual.word-fixes", "build-indexes");
        fn.invoke();
    }

    public static void loadPickle(String path, String type) {
        IFn fn = Clojure.var("conceptual.core", "load-pickle-0!");
        fn.invoke(path, type);
    }

    public static void load(String path) {
        run("conceptual.core");
        loadPickle(path, "r");
    }

    static Object derefVar(String name) {
        IFn var = Clojure.var("conceptual.core", name);
        if (var != null) {
            Object result = deref.invoke(var);
            if (result != null) {
                return result;
            } else {
                throw new RuntimeException("failed to deref var.");
            }
        } else {
            throw new RuntimeException("failed to locate var.");
        }
    }

    static Object derefAtom(String name) {
        Atom atom = (Atom) derefVar(name);
        Object result = deref.invoke(atom);
        if (result != null) {
            return result;
        } else {
            throw new RuntimeException("failed to deref atom.");
        }
    }


}
