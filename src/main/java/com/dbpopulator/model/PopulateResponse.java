package com.dbpopulator.model;

public record PopulateResponse(
    String jobId,
    String statusUrl,
    String error
) {
    public PopulateResponse(String jobId, String statusUrl) {
        this(jobId, statusUrl, null);
    }
}
