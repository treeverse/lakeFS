package io.lakefs;

public final class StringUtils {

    static String trimLeadingSlash(String s) {
        if (s.startsWith(Constants.SEPARATOR)) {
            return s.substring(1);
        }
        return s;
    }

    static String addLeadingSlash(String s) {
        return s.isEmpty() || s.endsWith(Constants.SEPARATOR) ? s : s + Constants.SEPARATOR;
    }
}
