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

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 *
 * @param <T> The type representing the field schemas.
 */
public interface Partitioner<T> {
  void configure(Map<String, Object> config);

  /**
   * Returns string representing the output path for a sinkRecord to be encoded and stored.
   *
   * @param sinkRecord The record to be stored by the Sink Connector
   * @return The path/filename the SinkRecord will be stored into after it is encoded
   */
  String encodePartition(SinkRecord sinkRecord);

  /**
   * Returns string representing the output path for a sinkRecord to be encoded and stored.
   *
   * @param sinkRecord The record to be stored by the Sink Connector
   * @param nowInMillis The current time in ms. Some Partitioners will use this option, but by
   *                    default it is unused.
   * @return The path/filename the SinkRecord will be stored into after it is encoded
   */
  default String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    return encodePartition(sinkRecord);
  }

  default String encodePartition(SinkRecord sinkRecord, long nowInMillis,
                                 Map<String, String> substitutionVariables) {
    String encodedPartition = encodePartition(sinkRecord, nowInMillis);

    // update the variables if the partition changed
    if (!encodedPartition.equals(substitutionVariables.get("partitioner.encodedPartition"))) {
      // set paths
      substitutionVariables.put("partitioner.encodedPartition", encodedPartition);
      substitutionVariables.put("partitioner.partitionedPath",
              generatePartitionedPath(sinkRecord.topic(), encodedPartition));

      // set millis
      substitutionVariables.put("partitioner.nowMillis", String.valueOf(nowInMillis));
    }

    return encodedPartition;
  }

  String generatePartitionedPath(String topic, String encodedPartition);

  List<T> partitionFields();
}
