// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


/**
 * JsonApi writer for response.
 */
public class JsonApiResponseWriter implements ResponseWriter {

    /**
     * Writes JSON-API response.
     * The response when serialized is in the following format:
     * <pre>
     * {@code{
     *   "rows" : [
     *     {
     *       "dateTime" : "YYYY-MM-dd HH:mm:ss.SSS",
     *       "logicalMetric1Name" : logicalMetric1Value,
     *       "logicalMetric2Name" : logicalMetric2Value,
     *       ...
     *       "dimension1Name" : "dimension1ValueKeyValue1",
     *       "dimension2Name" : "dimension2ValueKeyValue1",
     *       ...
     *     }, {
     *      ...
     *     }
     *   ],
     *   "dimension1Name" : [
     *     {
     *       "dimension1KeyFieldName" : "dimension1KeyFieldValue1",
     *       "dimension1OtherFieldName" : "dimension1OtherFieldValue1"
     *     }, {
     *       "dimension1KeyFieldName" : "dimension1KeyFieldValue2",
     *       "dimension1OtherFieldName" : "dimension1OtherFieldValue2"
     *     },
     *     ...
     *   ],
     *   "dimension2Name" : [
     *     {
     *       "dimension2KeyFieldName" : "dimension2KeyFieldValue1",
     *       "dimension2OtherFieldName" : "dimension2OtherFieldValue1"
     *     }, {
     *       "dimension2KeyFieldName" : "dimension2KeyFieldValue2",
     *       "dimension2OtherFieldName" : "dimension2OtherFieldValue2"
     *     },
     *     ...
     *   ]
     *   "linkName1" : "http://uri1",
     *   "linkName2": "http://uri2",
     *   ...
     *   "linkNameN": "http://uriN"
     * }
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

        try (JsonGenerator generator = responseData.getJsonFactory().createGenerator(os)) {
            // Holder for the dimension rows in the result set
            Map<Dimension, Set<Map<DimensionField, String>>> sidecars = new HashMap<>();
            for (DimensionColumn dimensionColumn :
                    responseData.getResultSet().getSchema().getColumns(DimensionColumn.class)) {
                sidecars.put(dimensionColumn.getDimension(), new LinkedHashSet<>());
            }

            // Start the top-level JSON object
            generator.writeStartObject();

            // Write the data rows and extract the dimension rows for the sidecars
            generator.writeArrayFieldStart("rows");
            for (Result result : responseData.getResultSet()) {
                generator.writeObject(responseData.buildResultRowWithSidecars(result, sidecars));
            }
            generator.writeEndArray();

            // Write the sidecar for each dimension
            for (Map.Entry<Dimension, Set<Map<DimensionField, String>>> sidecar : sidecars.entrySet()) {
                generator.writeArrayFieldStart(sidecar.getKey().getApiName());
                for (Map<DimensionField, String> dimensionRow : sidecar.getValue()) {

                    // Write each of the sidecar rows
                    generator.writeStartObject();
                    for (DimensionField dimensionField : dimensionRow.keySet()) {
                        generator.writeObjectField(dimensionField.getName(), dimensionRow.get(dimensionField));
                    }
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            }

            responseData.writeMetaObject(generator, responseData.getMissingIntervals(),
                    responseData.getVolatileIntervals(), responseData.getPagination());

            // End the top-level JSON object
            generator.writeEndObject();
        } catch (IOException e) {
            responseData.getLOG().error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }
}
