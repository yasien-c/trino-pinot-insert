/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.css.eventlistener;

import io.airlift.log.Logger;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public abstract class AbstractAsyncEventListener<T>
        implements EventListener
{
    private static final Logger log = Logger.get(AbstractAsyncEventListener.class);

    private final BlockingQueue<T> queue;
    private final AtomicBoolean started = new AtomicBoolean();
    private final ExecutorService executor;
    private final EventLogger<T> eventLogger;

    public AbstractAsyncEventListener(EventLogger eventLogger, int maxQueueLength)
    {
        checkArgument(maxQueueLength > 0, "maxQueueLength must be greater than zero");
        this.eventLogger = requireNonNull(eventLogger, "eventLogger is null");
        this.queue = new LinkedBlockingQueue<>(maxQueueLength);
        this.executor = newSingleThreadExecutor(daemonThreadsNamed(format("%s-query-event-logger", eventLogger.getName())));
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            executor.submit(new FlushTask());
        }
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        try {
            eventLogger.close();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        normalizeEvent(queryCreatedEvent).ifPresent(this::log);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        normalizeEvent(queryCompletedEvent).ifPresent(this::log);
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        normalizeEvent(splitCompletedEvent).ifPresent(this::log);
    }

    protected abstract Optional<T> normalizeEvent(QueryCreatedEvent event);

    protected abstract Optional<T> normalizeEvent(QueryCompletedEvent event);

    protected abstract Optional<T> normalizeEvent(SplitCompletedEvent event);

    protected void log(T event)
    {
        while (!queue.offer(event)) {
            log.warn("Buffer capacity exceeded. Dropping oldest entries to make room");
            queue.poll();
        }
    }

    class FlushTask
            implements Runnable
    {
        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    T event = queue.take();
                    eventLogger.log(event);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                catch (RuntimeException e) {
                    log.warn(e, "Unexpected flush task edception");
                }
            }
        }
    }
}
