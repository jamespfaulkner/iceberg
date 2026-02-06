/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;


@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotTableAction extends CatalogTestBase {
  private static final String SOURCE_NAME = "spark_catalog.default.source";

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s PURGE", SOURCE_NAME);
  }

  @TestTemplate
  public void testSnapshotWithParallelTasks() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

    AtomicInteger snapshotThreadsIndex = new AtomicInteger(0);
    SparkActions.get()
        .snapshotTable(SOURCE_NAME)
        .as(tableName)
        .executeWith(
            Executors.newFixedThreadPool(
                4,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("table-snapshot-" + snapshotThreadsIndex.getAndIncrement());
                  thread.setDaemon(true);
                  return thread;
                }))
        .execute();
    assertThat(snapshotThreadsIndex.get()).isEqualTo(2);
  }

    @TestTemplate
    public void testSnapshotAvroTableWithSchemaLiteralAndAliases() throws Exception {
        assumeThat(catalogName)
                .as("Avro SerDe requires Hive metastore")
                .isEqualTo("spark_catalog");

        String location = Files.createTempDirectory(temp, "junit").toFile().toString();

        //
        // this schema contains aliases
        //
        String avroSchema =
                "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"test_record\","
                        + "\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"long\",\"aliases\": [\"user_id\", \"ID\"]},"
                        + "{\"name\":\"data\",\"type\":[\"null\",\"string\"],\"default\":null, \"aliases\": [\"DATA\", \"dAtA\"]}"
                        + "]"
                        + "}";

        sql(
                "CREATE TABLE %s "
                        + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
                        + "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
                        + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' "
                        + "LOCATION '%s' "
                        + "TBLPROPERTIES ('avro.schema.literal'='%s')",
                SOURCE_NAME, location, avroSchema);

        sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
        sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

        List<Row> expected = spark.sql("SELECT * FROM " + SOURCE_NAME + " ORDER BY id").collectAsList();

        SparkActions.get().snapshotTable(SOURCE_NAME).as(tableName).execute();

        List<Row> actual = spark.sql("SELECT * FROM " + tableName + " ORDER BY id").collectAsList();

        assertThat(actual)
                .as("Snapshotted Iceberg table should contain the same data as source Avro table")
                .containsExactlyElementsOf(expected);

        TableCatalog catalog =
                (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
        SparkTable sparkTable =
                (SparkTable) catalog.loadTable(Spark3Util.catalogAndIdentifier(spark, tableName).identifier());
        Table icebergTable = sparkTable.table();

        assertThat(icebergTable.properties())
                .as("Iceberg table should have a name mapping for reading Avro files without field IDs")
                .containsKey(TableProperties.DEFAULT_NAME_MAPPING);

        //
        // The mapping does not contain aliases
        //
        NameMapping nameMapping = NameMappingParser.fromJson(icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
        assertThat(nameMapping.find(1).names()).containsExactlyElementsOf(Lists.newArrayList("id"));
        assertThat(nameMapping.find(2).names()).containsExactlyElementsOf(Lists.newArrayList("data"));
    }
}
