// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.Result;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Json writer for response.
 */
public class JsonResponseWriter implements ResponseWriter {

    /**
     * Writes JSON response.
     * The response when serialized is in the following format
     * <pre>
     * {@code
     * {
     *     "metricColumn1Name" : metricValue1,
     *     "metricColumn2Name" : metricValue2,
     *     .
     *     .
     *     .
     *     "dimensionColumnName" : "dimensionRowDesc",
     *     "dateTime" : "formattedDateTimeString"
     * },
     *   "linkName1" : "http://uri1",
     *   "linkName2": "http://uri2",
     *   ...
     *   "linkNameN": "http://uriN"
     * }
     * </pre>
     *
     * Where "linkName1" ... "linkNameN" are the N keys in paginationLinks, and "http://uri1" ... "http://uriN" are the
     * associated URI's.
     *
     * @param request Api request
     * @param responseData  data object containing all the result information
     * @param os  OutputStream
     *
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    public void write(
            ApiRequest request,
            ResponseData responseData,
            OutputStream os
    ) throws IOException {

        try (JsonGenerator g = responseData.getJsonFactory().createGenerator(os)) {
            g.writeStartObject();

            g.writeArrayFieldStart("rows");
            for (Result result : responseData.getResultSet()) {
                g.writeObject(responseData.buildResultRow(result));
            }
            g.writeEndArray();

            responseData.writeMetaObject(g, responseData.getMissingIntervals(),
                    responseData.getVolatileIntervals(), responseData.getPagination());

            g.writeEndObject();
        } catch (IOException e) {
            responseData.getLOG().error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }
}
