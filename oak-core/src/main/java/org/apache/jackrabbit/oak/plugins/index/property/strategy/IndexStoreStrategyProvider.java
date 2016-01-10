package org.apache.jackrabbit.oak.plugins.index.property.strategy;

public class IndexStoreStrategyProvider {
    private static final IndexStoreStrategy UNIQUE = new UniqueEntryStoreStrategy();
    private static final IndexStoreStrategy DEFAULT;

    static {
        if ("semi_flat".equals(System.getProperty("oak.index.strategy"))) {
            DEFAULT = new SemiFlatStoreStrategy(4);
        } else {
            DEFAULT = new ContentMirrorStoreStrategy();
        }
    }

    public static IndexStoreStrategy getStrategy(boolean unique) {
        return unique ? UNIQUE : DEFAULT;
    }
}
