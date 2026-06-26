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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.validation;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Result of a single validator (e.g. record count, similarity, sampling). */
@Data
@Builder
public class ValidationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String validatorName;
    private boolean passed;
    @Builder.Default private int totalChecked = 0;
    @Builder.Default private int failedCount = 0;
    @Builder.Default private List<String> details = new ArrayList<>();
    private String errorMessage;
    @Builder.Default private long durationMs = 0L;

    public void addDetail(String detail) {
        details.add(detail);
    }

    public void incrementFailed() {
        failedCount++;
    }

    public void incrementChecked() {
        totalChecked++;
    }
}
