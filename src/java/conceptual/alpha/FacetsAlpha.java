package conceptual.alpha;

import conceptual.core.DB;

public final class FacetsAlpha {

    private FacetsAlpha() {}

    /**
     * Returns an array where even indices are the relation value id and the immediate next index
     * is the count of that relation value id
     */
    public static int[] getToManyRelationsByFrequency(final DB db, final int[] ids, final int relationKey,
                                                      final int minId, final int maxId) {
        if (ids == null) return null;

        final int[] freqBins = new int[maxId + 1];

        int usedBins = 0;
        int[] relations;
        for (int i = 0; i < ids.length; i++) {
            relations = (int[]) db.getValue(ids[i], relationKey);
            if (relations != null) {
                for (int j = 0; j < relations.length; j++) {
                    int rel = relations[j];
                    if (freqBins[rel] == 0) usedBins++;
                    freqBins[rel]++;
                }
            }
        }

        final int[] result = new int[usedBins * 2];
        int r = 0;
        for (int i=0; i < freqBins.length; i++) {
            if (freqBins[i] > 0) {
                result[r] = i;
                result[r+1] = freqBins[i];
                r += 2;
            }
        }
        return result;
    }

    /**
     * Returns an array where even indices are the relation value id and the immediate next index
     * is the count of that relation value id
     */
    public static int[] getToOneRelationsByFrequency(final DB db, final int[] ids, final int relationKey,
                                                     final int minId, final int maxId) {
        if (ids == null) return null;

        final int[] freqBins = new int[maxId + 1];
        int usedBins = 0;
        int relation;
        Object temp;
        for (int i = 0; i < ids.length; i++) {
            temp = db.getValue(ids[i], relationKey);
            if (temp != null) {
                relation = (int) temp;
                if (freqBins[relation] == 0) usedBins++;
                freqBins[relation]++;
            }
        }

        final int[] result = new int[usedBins * 2];
        int r = 0;
        for (int i=0; i < freqBins.length; i++) {
            if (freqBins[i] > 0) {
                result[r] = i;
                result[r+1] = freqBins[i];
                r += 2;
            }
        }
        return result;
    }
}
