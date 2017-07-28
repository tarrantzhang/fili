// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


/**
 * Response writer selector object.
 */
public interface ResponseWriterSelector {
    /**
     * Select ResponseWriter given certain type of format from DataApiRequest.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @return  Writer of given format type
     */
    default ResponseWriter select(DataApiRequest request) {
        return new FiliResponseWriterSelector().select(request);
    }
}
