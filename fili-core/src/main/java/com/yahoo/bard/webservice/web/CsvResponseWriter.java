// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

/**
 * CSV writer for response.
 */
public class CsvResponseWriter implements ResponseWriter {
    /**
     * Writes CSV response.
     *
     * @param request  ApiRequest object with all the associated info in it
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
        CsvSchema schema = responseData.buildCsvHeaders();

        // Just write the header first
        responseData.getCsvMapper().writer().with(schema.withSkipFirstDataRow(true))
                .writeValue(os, Collections.emptyMap());

        ObjectWriter writer = responseData.getCsvMapper().writer().with(schema.withoutHeader());

        try {
            responseData.getResultSet().stream()
                    .map(responseData::buildResultRow)
                    .forEachOrdered(
                            row -> {
                                try {
                                    writer.writeValue(os, row);
                                } catch (IOException ioe) {
                                    String msg = String.format("Unable to write CSV data row: %s", row);
                                    responseData.getLOG().error(msg, ioe);
                                    throw new RuntimeException(msg, ioe);
                                }
                            }
                    );
        } catch (RuntimeException re) {
            throw new IOException(re);
        }
    }
}
