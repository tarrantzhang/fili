// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.io.IOException;
import java.io.OutputStream;


/**
 * Default writer for response. Will choose to use the writer type from format type.
 */
public class FiliResponseWriter implements ResponseWriter {

    private FiliResponseWriterSelector responseWriterSelector = new FiliResponseWriterSelector();

    /**
     * Selects a writer from mapping and writes response.
     *
     * @param request Api request
     * @param responseData  data object containing all the result information
     * @param os  OutputStream
     *
     * @throws IOException if a problem is encountered writing to the OutputStreamC
     */
    public void write(
            ApiRequest request,
            ResponseData responseData,
            OutputStream os
    ) throws IOException {
        ResponseWriter writer = responseWriterSelector.select(request);
        writer.write(request, responseData, os);
    }

    /**
     * Add ResponseFormatType to writer mapping.
     *
     * @param type Custom type
     * @param writer a writer map to the custom type
     */
    void addResponseType(ResponseFormatType type, ResponseWriter writer) {
        responseWriterSelector.getWriters().put(type, writer);
    }
}
