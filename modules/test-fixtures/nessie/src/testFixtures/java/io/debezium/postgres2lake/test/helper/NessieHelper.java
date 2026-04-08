package io.debezium.postgres2lake.test.helper;

public record NessieHelper(String apiBaseUrl) {
    public NessieHelper(String apiBaseUrl) {
        this.apiBaseUrl = apiBaseUrl.endsWith("/") ? apiBaseUrl.substring(0, apiBaseUrl.length() - 1) : apiBaseUrl;
    }
}
