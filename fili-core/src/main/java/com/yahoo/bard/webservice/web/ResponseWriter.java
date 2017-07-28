// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


import java.io.IOException;
import java.io.OutputStream;

/**
 * Response writer interface.
 */
public interface ResponseWriter {
    /**
     * Interface for all responseWriter. Default to use a FiliResponseWriter to writer response according to format
     * type from request.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @param responseData  Data object containing all the result information
     * @param os  OutputStream
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    default void write(ApiRequest request, ResponseData responseData, OutputStream os)throws IOException {
        new FiliResponseWriter().write(request, responseData, os);
    }
}
