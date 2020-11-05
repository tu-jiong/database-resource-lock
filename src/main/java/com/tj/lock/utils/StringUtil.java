package com.tj.lock.utils;

public class StringUtil {

    private StringUtil() {
    }

    /**
     * empty string
     */
    public static final String EMPTY = "";

    /**
     * Is empty boolean.
     *
     * @param str the str
     * @return the boolean
     */
    public static boolean isNullOrEmpty(String str) {
        return (str == null) || (str.isEmpty());
    }

    /**
     * Is blank string ?
     *
     * @param str the str
     * @return boolean boolean
     */
    public static boolean isBlank(String str) {
        int length;

        if ((str == null) || ((length = str.length()) == 0)) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is Not blank string ?
     *
     * @param str the str
     * @return boolean boolean
     */
    public static boolean isNotBlank(String str) {
        int length;

        if ((str == null) || ((length = str.length()) == 0)) {
            return false;
        }

        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Equals boolean.
     *
     * @param a the a
     * @param b the b
     * @return boolean
     */
    public static boolean equals(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equals(b);
    }

    /**
     * Equals ignore case boolean.
     *
     * @param a the a
     * @param b the b
     * @return the boolean
     */
    public static boolean equalsIgnoreCase(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equalsIgnoreCase(b);
    }

    /**
     * Trim string to null if empty("").
     *
     * @param str the String to be trimmed, may be null
     * @return the trimmed String
     */
    public static String trimToNull(final String str) {
        final String ts = trim(str);
        return isEmpty(ts) ? null : ts;
    }

    /**
     * Trim string, or null if string is null.
     *
     * @param str the String to be trimmed, may be null
     * @return the trimmed string, {@code null} if null String input
     */
    public static String trim(final String str) {
        return str == null ? null : str.trim();
    }

    /**
     * Checks if a CharSequence is empty ("") or null.
     *
     * @param cs the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is empty or null
     */
    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * Checks if a CharSequence is not empty ("") and not null.
     *
     * @param cs the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is not empty and not null
     */
    public static boolean isNotEmpty(final CharSequence cs) {
        return !isEmpty(cs);
    }
}
