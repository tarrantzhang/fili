// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory;
import com.yahoo.bard.webservice.util.DateTimeUtils;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.StreamUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.core.StreamingOutput;

/**
 * Response class.
 */
public class ResponseData {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseData.class);
    private static final Map<Dimension, Map<DimensionField, String>> DIMENSION_FIELD_COLUMN_NAMES = new HashMap<>();

    private final ResultSet resultSet;
    private final LinkedHashSet<MetricColumn> apiMetricColumns;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields;
    private final ResponseFormatType responseFormatType;
    private final SimplifiedIntervalList missingIntervals;
    private final SimplifiedIntervalList volatileIntervals;
    private final Map<String, URI> paginationLinks;
    private final JsonFactory jsonFactory;
    private final Pagination pagination;
    private final CsvMapper csvMapper;
    private final ApiRequest apiRequest;
    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiMetricColumnNames  The names of the logical metrics requested
     * @param requestedApiDimensionFields  The fields for each dimension that should be shown in the response
     * @param responseFormatType  The format in which the response should be returned to the user
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param objectMappers  Suite of Object Mappers to use when serializing
     * @param apiRequest  API Request to get the metric columns from
     */
    public ResponseData(
            ResultSet resultSet,
            LinkedHashSet<String> apiMetricColumnNames,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields,
            ResponseFormatType responseFormatType,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Map<String, URI> paginationLinks,
            Pagination pagination,
            ObjectMappersSuite objectMappers,
            ApiRequest apiRequest
    ) {
        this.resultSet = resultSet;
        this.apiMetricColumns = generateApiMetricColumns(apiMetricColumnNames);
        this.requestedApiDimensionFields = requestedApiDimensionFields;
        this.responseFormatType = responseFormatType;
        this.missingIntervals = missingIntervals;
        this.volatileIntervals = volatileIntervals;
        this.paginationLinks = paginationLinks;
        this.pagination = pagination;
        this.jsonFactory = new JsonFactory(objectMappers.getMapper());
        this.csvMapper = objectMappers.getCsvMapper();
        this.apiRequest = apiRequest;

        LOG.trace("Initialized with ResultSet: {}", this.resultSet);
    }

    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiRequest  API Request to get the metric columns from
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param objectMappers  Suite of Object Mappers to use when serializing
     *
     * @deprecated  All the values needed to build a Response should be passed explicitly instead of relying on the
     * DataApiRequest
     */
    @Deprecated
    public ResponseData(
            ResultSet resultSet,
            DataApiRequest apiRequest,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Map<String, URI> paginationLinks,
            Pagination pagination,
            ObjectMappersSuite objectMappers
    ) {
        this(
                resultSet,
                apiRequest.getLogicalMetrics().stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toCollection(LinkedHashSet<String>::new)),
                apiRequest.getDimensionFields(),
                apiRequest.getFormat(),
                missingIntervals,
                volatileIntervals,
                paginationLinks,
                pagination,
                objectMappers,
                apiRequest
        );
    }

    public JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public SimplifiedIntervalList getMissingIntervals() {
        return missingIntervals;
    }

    public SimplifiedIntervalList getVolatileIntervals() {
        return volatileIntervals;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public static Logger getLOG() {
        return LOG;
    }

    public CsvMapper getCsvMapper() {
        return csvMapper;
    }

    /**
     * Writes the response string in the proper format.
     *
     * @param os  The output stream to write the response bytes to
     *
     * @throws IOException if writing to output stream fails
     */
    public void write(OutputStream os) throws IOException {
        ResponseWriter writer = new FiliResponseWriterSelector().select(apiRequest);
        writer.write(apiRequest, this, os);
    }

    /**
     * Builds a set of only those metric columns which correspond to the metrics requested in the API.
     *
     * @param apiMetricColumnNames  Set of Metric names extracted from the requested api metrics
     *
     * @return set of metric columns
     */
    private LinkedHashSet<MetricColumn> generateApiMetricColumns(Set<String> apiMetricColumnNames) {
        // Get the metric columns from the schema
        Map<String, MetricColumn> metricColumnMap = resultSet.getSchema().getColumns(MetricColumn.class).stream()
                .collect(StreamUtils.toLinkedDictionary(MetricColumn::getName));

        // Select only api metrics from resultSet
        return apiMetricColumnNames.stream()
                .map(metricColumnMap::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Builds the CSV header.
     *
     * @return The CSV schema with the header
     */
    protected CsvSchema buildCsvHeaders() {
        CsvSchema.Builder builder = CsvSchema.builder();
        Stream.concat(
                Stream.of("dateTime"),
                Stream.concat(
                        requestedApiDimensionFields.entrySet().stream().flatMap(this::generateDimensionColumnHeaders),
                        apiMetricColumns.stream().map(MetricColumn::getName)
                )
        ).forEachOrdered(builder::addColumn);
        return builder.setUseHeader(true).build();
    }

    /**
     * Build the headers for the dimension columns.
     *
     * @param entry  Entry to base the columns on.
     *
     * @return the headers as a Stream
     */
    private Stream<String> generateDimensionColumnHeaders(Map.Entry<Dimension, LinkedHashSet<DimensionField>> entry) {
        if (entry.getValue().isEmpty()) {
            return Stream.of(entry.getKey().getApiName());
        } else {
            return entry.getValue().stream().map(dimField -> getDimensionColumnName(entry.getKey(), dimField));
        }
    }

    /**
     * Builds map of result row from a result.
     *
     * @param result  The result to process
     *
     * @return map of result row
     */
    public Map<String, Object> buildResultRow(Result result) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("dateTime", result.getTimeStamp().toString(DateTimeFormatterFactory.getOutputFormatter()));

        // Loop through the Map<DimensionColumn, DimensionRow> and format it to dimensionColumnName : dimensionRowDesc
        Map<DimensionColumn, DimensionRow> dr = result.getDimensionRows();
        for (Entry<DimensionColumn, DimensionRow> dce : dr.entrySet()) {
            DimensionRow drow = dce.getValue();
            Dimension dimension = dce.getKey().getDimension();

            Set<DimensionField> requestedDimensionFields;
            if (requestedApiDimensionFields.get(dimension) != null) {
                requestedDimensionFields = requestedApiDimensionFields.get(dimension);

                // When no fields are requested, show the key field
                if (requestedDimensionFields.isEmpty()) {
                    // When no fields are requested, show the key field
                    row.put(dimension.getApiName(), drow.get(dimension.getKey()));
                } else {
                    // Otherwise, show the fields requested, with the pipe-separated name
                    for (DimensionField dimensionField : requestedDimensionFields) {
                        row.put(getDimensionColumnName(dimension, dimensionField), drow.get(dimensionField));
                    }
                }
            }
        }

        // Loop through the Map<MetricColumn, Object> and format it to a metricColumnName: metricValue map
        for (MetricColumn apiMetricColumn : apiMetricColumns) {
            row.put(apiMetricColumn.getName(), result.getMetricValue(apiMetricColumn));
        }
        return row;
    }

    /**
     * Builds map of result row from a result and loads the dimension rows into the sidecar map.
     *
     * @param result  The result to process
     * @param sidecars  Map of sidecar data (dimension rows in the result)
     *
     * @return map of result row
     */
    public Map<String, Object> buildResultRowWithSidecars(
            Result result,
            Map<Dimension, Set<Map<DimensionField, String>>> sidecars
    ) {

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("dateTime", result.getTimeStamp().toString(DateTimeFormatterFactory.getOutputFormatter()));

        // Loop through the Map<DimensionColumn, DimensionRow> and format it to dimensionColumnName : dimensionRowKey
        Map<DimensionColumn, DimensionRow> dr = result.getDimensionRows();
        for (Entry<DimensionColumn, DimensionRow> dimensionColumnEntry : dr.entrySet()) {
            // Get the pieces we need out of the map entry
            Dimension dimension = dimensionColumnEntry.getKey().getDimension();
            DimensionRow dimensionRow = dimensionColumnEntry.getValue();

            if (requestedApiDimensionFields.get(dimension) != null) {
                // add sidecar only if at-least one field needs to be shown
                Set<DimensionField> requestedDimensionFields = requestedApiDimensionFields.get(dimension);
                if (requestedDimensionFields.size() > 0) {
                    // The key field is required
                    requestedDimensionFields.add(dimension.getKey());

                    //
                    Map<DimensionField, String> dimensionFieldToValueMap = requestedDimensionFields.stream()
                            .collect(StreamUtils.toLinkedMap(Function.identity(), dimensionRow::get));

                    // Add the dimension row's requested fields to the sidecar map
                    sidecars.get(dimension).add(dimensionFieldToValueMap);
                }
            }

            // Put the dimension name and dimension row's key value into the row map
            row.put(dimension.getApiName(), dimensionRow.get(dimension.getKey()));
        }

        // Loop through the Map<MetricColumn, Object> and format it to a metricColumnName: metricValue map
        for (MetricColumn apiMetricColumn : apiMetricColumns) {
            row.put(apiMetricColumn.getName(), result.getMetricValue(apiMetricColumn));
        }
        return row;
    }

    /**
     * Build a list of interval strings. Format of interval string: yyyy-MM-dd' 'HH:mm:ss/yyyy-MM-dd' 'HH:mm:ss
     *
     * @param intervals  list of intervals to be converted into string
     *
     * @return list of interval strings
     */
    private List<String> buildIntervalStringList(Collection<Interval> intervals) {
        return intervals.stream()
                .map(it -> DateTimeUtils.intervalToString(it, DateTimeFormatterFactory.getOutputFormatter(), "/"))
                .collect(Collectors.toList());
    }

    /**
     * Retrieve dimension column name from cache, or build it and cache it.
     *
     * @param dimension  The dimension for the column name
     * @param dimensionField  The dimensionField for the column name
     *
     * @return The name for the dimension and column as it will appear in the response document
     */
    private static String getDimensionColumnName(Dimension dimension, DimensionField dimensionField) {
        Map<DimensionField, String> columnNamesForDimensionFields;
        columnNamesForDimensionFields = DIMENSION_FIELD_COLUMN_NAMES.computeIfAbsent(
                dimension,
                (key) -> new ConcurrentHashMap()
        );
        return columnNamesForDimensionFields.computeIfAbsent(
                dimensionField, (field) -> dimension.getApiName() + "|" + field.getName()
        );
    }

    /**
     * Builds the meta object for the JSON response. The meta object is only built if there were missing intervals, or
     * the results are being paginated.
     *
     * @param generator  The JsonGenerator used to build the JSON response.
     * @param missingIntervals  The set of intervals that do not contain data.
     * @param volatileIntervals  The set of intervals that have volatile data.
     * @param pagination  Object containing the pagination metadata (i.e. the number of rows per page, and the requested
     * page)
     *
     * @throws IOException if the generator throws an IOException.
     */
    public void writeMetaObject(
            JsonGenerator generator,
            Collection<Interval> missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination
    ) throws IOException {
        boolean paginating = pagination != null;
        boolean haveMissingIntervals = BardFeatureFlag.PARTIAL_DATA.isOn() && !missingIntervals.isEmpty();
        boolean haveVolatileIntervals = volatileIntervals != null && ! volatileIntervals.isEmpty();

        if (!paginating && !haveMissingIntervals && !haveVolatileIntervals) {
            return;
        }

        generator.writeObjectFieldStart("meta");

        // Add partial data info into the metadata block if needed.
        if (haveMissingIntervals) {
            generator.writeObjectField("missingIntervals", buildIntervalStringList(missingIntervals));
        }

        // Add volatile intervals
        if (haveVolatileIntervals) {
            generator.writeObjectField("volatileIntervals", buildIntervalStringList(volatileIntervals));
        }

        // Add pagination information if paginating.
        if (paginating) {
            generator.writeObjectFieldStart("pagination");

            for (Entry<String, URI> entry : paginationLinks.entrySet()) {
                generator.writeObjectField(entry.getKey(), entry.getValue());
            }

            generator.writeNumberField("currentPage", pagination.getPage());
            generator.writeNumberField("rowsPerPage", pagination.getPerPage());
            generator.writeNumberField("numberOfResults", pagination.getNumResults());

            generator.writeEndObject();
        }

        generator.writeEndObject();
    }

    /**
     * Get a resource method that can be used to stream this response as an entity.
     *
     * @return The resource method
     */
    public StreamingOutput getResponseStream() {
        return this::write;
    }
}
