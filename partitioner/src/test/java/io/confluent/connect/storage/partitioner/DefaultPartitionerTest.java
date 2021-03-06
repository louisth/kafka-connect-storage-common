/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DefaultPartitionerTest extends StorageSinkTestBase {

  @Test
  public void testDefaultPartitioner() {
    Map<String, Object> config = new HashMap<>();
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

    DefaultPartitioner<String> partitioner = new DefaultPartitioner<>();
    partitioner.configure(config);

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    long timestamp = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecord(timestamp);
    String encodedPartition = partitioner.encodePartition(sinkRecord);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("partition", PARTITION);
    assertThat(encodedPartition, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testDefaultPartitionerVariables() {
    Map<String, Object> config = new HashMap<>();
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

    DefaultPartitioner<String> partitioner = new DefaultPartitioner<>();
    partitioner.configure(config);

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    long timestamp = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecord(timestamp);
    long nowInMillis = 10001;

    Map<String, String> substitutionVariables = new HashMap<>();
    String encodedPartition = partitioner.encodePartition(sinkRecord, nowInMillis, substitutionVariables);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("partition", PARTITION);
    assertThat(encodedPartition, is(generateEncodedPartitionFromMap(m)));

    m.clear();
    m.put("partitioner.encodedPartition", "partition=" + PARTITION);
    m.put("partitioner.partitionedPath", TOPIC + StorageCommonConfig.DIRECTORY_DELIM_DEFAULT + "partition=" + PARTITION);
    m.put("partitioner.nowMillis", "10001");
    assertThat(substitutionVariables, is(m));
  }

}
