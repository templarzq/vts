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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class VectorValidationConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String pgUrl;
    private String pgUser;
    private String pgPassword;
    private String pgTable;
    private int sampleSize;
    private double similarityThreshold;

    public static VectorValidationConfig of(ReadonlyConfig config) {
        VectorValidationConfig c = new VectorValidationConfig();
        c.setPgUrl(config.get(VectorValidationTransformFactory.PG_URL));
        c.setPgUser(config.get(VectorValidationTransformFactory.PG_USER));
        c.setPgPassword(config.get(VectorValidationTransformFactory.PG_PASSWORD));
        c.setPgTable(config.get(VectorValidationTransformFactory.PG_TABLE));
        c.setSampleSize(config.get(VectorValidationTransformFactory.SAMPLE_SIZE));
        c.setSimilarityThreshold(config.get(VectorValidationTransformFactory.SIMILARITY_THRESHOLD));
        return c;
    }
}
