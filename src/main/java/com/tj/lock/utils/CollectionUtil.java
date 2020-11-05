package com.tj.lock.utils;

import java.util.Collection;

public class CollectionUtil {

    private CollectionUtil() {
    }

    /**
     * Is empty boolean.
     *
     * @param col the col
     * @return the boolean
     */
    public static boolean isEmpty(Collection<?> col) {
        return !isNotEmpty(col);
    }

    /**
     * Is not empty boolean.
     *
     * @param col the col
     * @return the boolean
     */
    public static boolean isNotEmpty(Collection<?> col) {
        return col != null && !col.isEmpty();
    }

    /**
     * Is empty boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isEmpty(Object[] array) {
        return !isNotEmpty(array);
    }

    /**
     * Is not empty boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isNotEmpty(Object[] array) {
        return array != null && array.length > 0;
    }

    /**
     * Is size equals boolean.
     *
     * @param col0 the col 0
     * @param col1 the col 1
     * @return the boolean
     */
    public static boolean isSizeEquals(Collection<?> col0, Collection<?> col1) {
        if (col0 == null) {
            return col1 == null;
        } else {
            if (col1 == null) {
                return false;
            } else {
                return col0.size() == col1.size();
            }
        }
    }

}
