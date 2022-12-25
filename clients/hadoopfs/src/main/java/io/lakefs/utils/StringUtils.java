package io.lakefs.utils;

import io.lakefs.Constants;

public final class StringUtils {

    public static String trimLeadingSlash(String s) {
        if (s.startsWith(Constants.SEPARATOR)) {
            return s.substring(1);
        }
        return s;
    }

    public static String addLeadingSlash(String s) {
        return s.isEmpty() || s.endsWith(Constants.SEPARATOR) ? s : s + Constants.SEPARATOR;
    }
}
