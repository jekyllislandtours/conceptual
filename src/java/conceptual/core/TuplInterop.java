package conceptual.core;

import clojure.java.api.Clojure;

import clojure.lang.IFn;
import clojure.lang.Keyword;

import org.cojen.tupl.Database;
import org.cojen.tupl.Index;

public class TuplInterop {

    public static RDB getRDB(final Keyword k) {
        IFn fn = Clojure.var("conceptual.core", "empty-db");
        return (RDB) ((PersistentDB) fn.invoke(k)).compactToRDB();
    }

    public static Database getDatabase(final Keyword k) {
        IFn fn = Clojure.var("conceptual.tupl", "db");
        return (Database) fn.invoke(k);
    }

    public static Index getIndex(final Keyword dbKey, final Keyword indexKey) {
        IFn fn = Clojure.var("conceptual.tupl", "index");
        return (Index) fn.invoke(dbKey, indexKey);
    }

}
