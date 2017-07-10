// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels;

import io.reactivex.Observable;
import io.reactivex.subjects.Subject;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Singleton;

/**
 * A simple implementation of BroadcastChannel backed by a Subject intended for use in systems with a single Bard
 * instance.
 *
 * @param <T>  The type of message that the SimpleBroadcastChannel publishes
 */
@Singleton
public class SimpleBroadcastChannel<T> implements BroadcastChannel<T> {

    private final Subject<T> notifications;
    private final ReadWriteLock isClosedLock;
    private volatile boolean isClosed;

    /**
     * Construct a SimpleBroadcastChannel using a Hot Observable.
     *
     * @param notifications  A hot subject that will be used to send messages to and receive messages from other
     * SimpleBroadcastChannels
     */
    public SimpleBroadcastChannel(Subject<T> notifications) {
        this.notifications = notifications;
        this.isClosedLock = new ReentrantReadWriteLock();
        this.isClosed = false;
    }

    @Override
    public void publish(T message) throws UnsupportedOperationException {
        isClosedLock.readLock().lock();
        try {
            if (isClosed) {
                throw new UnsupportedOperationException(PUBLISH_ON_CLOSED_ERROR_MESSAGE);
            }
            notifications.onNext(message);
        } finally {
            isClosedLock.readLock().unlock();
        }
    }

    @Override
    public Observable<T> getNotifications() {
        return notifications;
    }

    @Override
    public void close() {
        isClosedLock.writeLock().lock();
        try {
            if (!isClosed) {
                isClosed = true;
                notifications.onComplete();
            }
        } finally {
            isClosedLock.writeLock().unlock();
        }
    }
}
