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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class VectorValidationTransformFactory implements TableTransformFactory {

    public static final Option<String> PG_URL =
            Options.key("pg_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PostgreSQL JDBC URL for validation queries");

    public static final Option<String> PG_USER =
            Options.key("pg_user").stringType().noDefaultValue().withDescription("PostgreSQL user");

    public static final Option<String> PG_PASSWORD =
            Options.key("pg_password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("PostgreSQL password");

    public static final Option<String> PG_TABLE =
            Options.key("pg_table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("pgvector table (schema.table) for validation queries");

    public static final Option<Integer> SAMPLE_SIZE =
            Options.key("sample_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of rows to sample for vector similarity validation");

    public static final Option<Double> SIMILARITY_THRESHOLD =
            Options.key("similarity_threshold")
                    .doubleType()
                    .defaultValue(0.9999)
                    .withDescription("Minimum cosine similarity for a row to pass validation");

    @Override
    public String factoryIdentifier() {
        return "VectorValidation";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PG_URL, PG_USER, PG_TABLE)
                .optional(PG_PASSWORD, SAMPLE_SIZE, SIMILARITY_THRESHOLD)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        VectorValidationConfig config = VectorValidationConfig.of(options);
        return () -> new VectorValidationTransform(config, context.getCatalogTables().get(0));
    }
}
