/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/** Aggregates {@link SaveModeHandler}s for all underlying sinks wrapped by {@link MultiTableSink}. */
@Slf4j
public class MultiTableSaveModeHandler implements SaveModeHandler {

    private final List<SaveModeHandler> handlers;

    public MultiTableSaveModeHandler(List<SaveModeHandler> handlers) {
        if (handlers == null || handlers.isEmpty()) {
            throw new IllegalArgumentException("handlers cannot be null or empty");
        }
        this.handlers = handlers;
    }

    @Override
    public void open() {
        for (SaveModeHandler handler : handlers) {
            handler.open();
        }
    }

    @Override
    public void handleSchemaSaveMode() {
        for (SaveModeHandler handler : handlers) {
            handler.handleSchemaSaveMode();
        }
    }

    @Override
    public void handleDataSaveMode() {
        for (SaveModeHandler handler : handlers) {
            handler.handleDataSaveMode();
        }
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return handlers.get(0).getSchemaSaveMode();
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return handlers.get(0).getDataSaveMode();
    }

    @Override
    public TablePath getHandleTablePath() {
        return handlers.get(0).getHandleTablePath();
    }

    @Override
    public Catalog getHandleCatalog() {
        return handlers.get(0).getHandleCatalog();
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (SaveModeHandler handler : handlers) {
            try {
                handler.close();
            } catch (Exception e) {
                log.warn("Failed to close save mode handler for {}", handler.getHandleTablePath(), e);
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
