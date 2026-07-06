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

package org.apache.seatunnel.connectors.migration.milvus2pgvector.transform;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class MilvusToPgVectorTransformConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Option<String> PG_SCHEMA =
            Options.key("pg_schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("pgvector target schema name");

    public static final Option<String> PG_TABLE =
            Options.key("pg_table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "pgvector target table name; defaults to the Milvus collection name");

    private String pgSchema;
    private String pgTable;

    public static MilvusToPgVectorTransformConfig of(ReadonlyConfig config) {
        MilvusToPgVectorTransformConfig c = new MilvusToPgVectorTransformConfig();
        c.setPgSchema(config.get(PG_SCHEMA));
        c.setPgTable(config.get(PG_TABLE));
        return c;
    }
}
