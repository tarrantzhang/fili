// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


import java.util.HashMap;
import java.util.Map;

/**
 * Response writer selector object.
 */
public class FiliResponseWriterSelector implements ResponseWriterSelector {
    private Map<ResponseFormatType, ResponseWriter> writers;

    /**
     * Constructor for default writer selector. Initialize format to writer mapping.
     */
    public FiliResponseWriterSelector() {
        writers = new HashMap<>();
        writers.put(ResponseFormatType.CSV, new CsvResponseWriter());
        writers.put(ResponseFormatType.JSONAPI, new JsonApiResponseWriter());
        writers.put(ResponseFormatType.JSON, new JsonResponseWriter());
    }

    /**
     * Selects a ReponseWriter given the format type from request.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @return Response writer for the given format type
     */
    public ResponseWriter select(ApiRequest request) {
        ResponseFormatType format = request.getFormat();
        if (format == null) {
            format = ResponseFormatType.JSON;
        }
        return writers.get(format);
    }

    public Map<ResponseFormatType, ResponseWriter> getWriters() {
        return writers;
    }
}
