package io.lakefs;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class FSConfiguration {

    private static String formatFSConfigurationKey(String scheme, String key) {
        return "fs." + scheme + "." + key;
    }

    /**
     * lookup value from configuration based on scheme and key suffix.
     * first try to get "fs.[scheme].[key suffix]", if value not found use the default scheme
     * to build a key for lookup.
     *
     * @param conf      configuration object to get the value from
     * @param scheme    used to format the key for lookup
     * @param keySuffix key suffix to lookup
     * @return key value or null in case no value found
     */
    public static String get(Configuration conf, String scheme, String keySuffix) {
        String key = formatFSConfigurationKey(scheme, keySuffix);
        String value = conf.get(key);
        if (value == null && !scheme.equals(Constants.DEFAULT_SCHEME)) {
            key = formatFSConfigurationKey(Constants.DEFAULT_SCHEME, keySuffix);
            value = conf.get(key);
        }
        return value;
    }

    /**
     * lookup value from configuration based on scheme and key suffix, returns default in case of null value.
     *
     * @param conf         configuration object to get the value from
     * @param scheme       used to format key for lookup
     * @param keySuffix    key suffix to lookup
     * @param defaultValue default value returned in case of null
     * @return value found or default value
     */
    public static String get(Configuration conf, String scheme, String keySuffix, String defaultValue) {
        String value = get(conf, scheme, keySuffix);
        return (value == null) ? defaultValue : value;
    }

    public static int getInt(Configuration conf, String scheme, String keySuffix, int defaultValue) {
        String valueString = get(conf, scheme, keySuffix);
        return (valueString == null) ? defaultValue : Integer.parseInt(valueString);
    }
    /**
     * lookup a map in a configuration key.
     * The map is expected to be in the format of "key1:value1,key2:value2"
     *
     * @param conf         configuration object to get the value from
     * @param scheme       used to format key for lookup
     * @param keySuffix    key suffix to lookup
     * @return value found or default value
     */
    public static Map<String,String> getMap(Configuration conf, String scheme, String keySuffix) {
        String valueString = get(conf, scheme, keySuffix);
        if (valueString == null) {
            return null;
        }
        return Arrays.stream(valueString.split(","))
                .map(entry -> entry.split(":"))
                .collect(Collectors.toMap(
                        entry -> entry[0],
                        entry -> entry[1]
                ));
    }
}
