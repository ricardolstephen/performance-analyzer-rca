package com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;

public class BatchMetricsDB extends MetricsDB {
    private static final String DB_FILE_PREFIX_PATH_DEFAULT = "/tmp/batchdb_";
    private static final String DB_FILE_PREFIX_PATH_CONF_NAME = "batch-db-file-prefix-path";

    public BatchMetricsDB(long windowStartTime) throws Exception {
        super(windowStartTime);
    }

    public String getDBFilePath() {
        return PluginSettings.instance()
                .getSettingValue(DB_FILE_PREFIX_PATH_CONF_NAME, DB_FILE_PREFIX_PATH_DEFAULT)
                + Long.toString(windowStartTime);
    }
}
