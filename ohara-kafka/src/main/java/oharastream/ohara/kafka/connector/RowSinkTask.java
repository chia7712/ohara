/*
 * Copyright 2019 is-land
 *
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

package oharastream.ohara.kafka.connector;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.common.util.VersionUtils;
import oharastream.ohara.kafka.TimestampType;
import oharastream.ohara.metrics.basic.Counter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * A wrap to Kafka SinkTask. It used to convert the Kafka SinkRecord to ohara RowSinkRecord. Ohara
 * developers should extend this class rather than kafka SinkTask in order to let the conversion
 * from SinkRecord to RowSinkRecord work automatically.
 */
public abstract class RowSinkTask extends SinkTask {

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup from the task.
   *
   * @param config initial configuration
   */
  protected abstract void run(TaskSetting config);

  /**
   * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once
   * outstanding calls to other methods have completed (e.g., put() has returned) and a final
   * flush() and offset commit has completed. Implementations from this method should only need to
   * perform final cleanup operations, such as closing network connections to the sink system.
   */
  protected abstract void terminate();

  /**
   * Put the table record in the sink. Usually this should send the records to the sink
   * asynchronously and immediately return.
   *
   * @param records table record
   */
  protected abstract void putRecords(List<RowSinkRecord> records);

  /**
   * The SinkTask use this method to create writers for newly assigned partitions in case from
   * partition rebalance. This method will be called after partition re-assignment completes and
   * before the SinkTask starts fetching data. Note that any errors raised from this method will
   * cause the task to stop.
   *
   * @param partitions The list from partitions that are now assigned to the task (may include
   *     partitions previously assigned to the task)
   */
  protected void openPartitions(List<TopicPartition> partitions) {
    // do nothing
  }

  /**
   * The SinkTask use this method to close writers for partitions that are no longer assigned to the
   * SinkTask. This method will be called before a rebalance operation starts and after the SinkTask
   * stops fetching data. After being closed, Connect will not write any records to the task until a
   * new set from partitions has been opened. Note that any errors raised from this method will
   * cause the task to stop.
   *
   * @param partitions The list from partitions that should be closed
   */
  protected void closePartitions(List<TopicPartition> partitions) {
    // do nothing
  }

  /**
   * Pre-commit hook invoked prior to an offset commit.
   *
   * <p>The default implementation simply return the offsets and is thus able to assume all offsets
   * are safe to commit.
   *
   * @param offsets the current offset state as from the last call to put, provided for convenience
   *     but could also be determined by tracking all offsets included in the RowSourceRecord's
   *     passed to put.
   * @return an empty map if Connect-managed offset commit is not desired, otherwise a map from
   *     offsets by topic-partition that are safe to commit.
   */
  protected Map<TopicPartition, TopicOffset> preCommitOffsets(
      Map<TopicPartition, TopicOffset> offsets) {
    return offsets;
  }

  protected RowSinkContext rowContext;
  // -------------------------------------------------[WRAPPED]-------------------------------------------------//
  @VisibleForTesting Counter messageNumberCounter = null;
  @VisibleForTesting Counter messageSizeCounter = null;
  @VisibleForTesting Counter ignoredMessageNumberCounter = null;
  @VisibleForTesting Counter ignoredMessageSizeCounter = null;
  @VisibleForTesting TaskSetting taskSetting = null;

  /**
   * @param record kafka's sink record
   * @return ohara's sink record
   */
  private static RowSinkRecord toOhara(SinkRecord record) {
    return RowSinkRecord.builder()
        .topicKey(TopicKey.requirePlain(record.topic()))
        // add a room to accept the row in kafka
        .row(
            (record.key() instanceof Row)
                ? ((Row) record.key())
                : Serializer.ROW.from((byte[]) record.key()))
        .partition(record.kafkaPartition())
        .offset(record.kafkaOffset())
        // constructing a record without timeout is legal in kafka ...
        .timestamp(record.timestamp() == null ? 0 : record.timestamp())
        .timestampType(TimestampType.of(record.timestampType()))
        .build();
  }

  @Override
  public final void put(Collection<SinkRecord> raw) {
    SettingDef.CheckRule rule = taskSetting.checkRule();
    List<Column> columns = taskSetting.columns();
    if (raw == null) return;
    List<RowSinkRecord> records =
        raw.stream()
            .map(kafkaRecord -> Map.entry(toOhara(kafkaRecord), kafkaRecord))
            .filter(
                pair -> {
                  long rowSize = ConnectorUtils.sizeOf(pair.getValue());
                  boolean pass =
                      ConnectorUtils.match(
                          rule,
                          pair.getKey().row(),
                          rowSize,
                          columns,
                          true,
                          ignoredMessageNumberCounter,
                          ignoredMessageSizeCounter);
                  if (pass && messageSizeCounter != null) messageSizeCounter.addAndGet(rowSize);

                  return pass;
                })
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableList());
    if (messageNumberCounter != null) messageNumberCounter.addAndGet(records.size());
    putRecords(records);
  }

  /**
   * create counter builder. This is a helper method for custom connector which want to expose some
   * number via ohara's metrics. NOTED: THIS METHOD MUST BE USED AFTER STARTING THIS CONNECTOR.
   * otherwise, an IllegalArgumentException will be thrown.
   *
   * @return counter
   */
  protected CounterBuilder counterBuilder() {
    if (taskSetting == null)
      throw new IllegalArgumentException("you can't create a counter before starting connector");
    return CounterBuilder.of().key(taskSetting.connectorKey());
  }

  @Override
  public final void start(Map<String, String> props) {
    taskSetting = TaskSetting.of(Collections.unmodifiableMap(props));
    messageNumberCounter = ConnectorUtils.messageNumberCounter(taskSetting.connectorKey());
    messageSizeCounter = ConnectorUtils.messageSizeCounter(taskSetting.connectorKey());
    ignoredMessageNumberCounter =
        ConnectorUtils.ignoredMessageNumberCounter(taskSetting.connectorKey());
    ignoredMessageSizeCounter =
        ConnectorUtils.ignoredMessageSizeCounter(taskSetting.connectorKey());
    run(taskSetting);
  }

  @Override
  public final void stop() {
    try {
      terminate();
    } finally {
      Releasable.close(messageNumberCounter);
      Releasable.close(messageSizeCounter);
      Releasable.close(ignoredMessageNumberCounter);
      Releasable.close(ignoredMessageSizeCounter);
    }
  }

  @Override
  public final String version() {
    return VersionUtils.VERSION;
  }

  @Override
  public final void open(Collection<org.apache.kafka.common.TopicPartition> partitions) {
    openPartitions(
        partitions.stream().map(TopicPartition::of).collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public final void close(Collection<org.apache.kafka.common.TopicPartition> partitions) {
    closePartitions(
        partitions.stream().map(TopicPartition::of).collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public final Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> preCommit(
      Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> currentOffsets) {

    return preCommitOffsets(
            currentOffsets.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        x -> TopicPartition.of(x.getKey()),
                        x -> new TopicOffset(x.getValue().metadata(), x.getValue().offset()))))
        .entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                x ->
                    new org.apache.kafka.common.TopicPartition(
                        x.getKey().topicKey().topicNameOnKafka(), x.getKey().partition()),
                x -> new OffsetAndMetadata(x.getValue().offset(), x.getValue().metadata())));
  }
  // -------------------------------------------------[UN-OVERRIDE]-------------------------------------------------//

  @Override
  public final void initialize(SinkTaskContext context) {
    super.initialize(context);
    rowContext = RowSinkContext.toRowSinkContext(context);
  }

  @SuppressWarnings({
    "deprecation",
    "kafka had deprecated this method but it still allow developer to override it. We forbrid the inherance now"
  })
  @Override
  public final void onPartitionsAssigned(
      Collection<org.apache.kafka.common.TopicPartition> partitions) {
    super.onPartitionsAssigned(partitions);
  }

  @SuppressWarnings({
    "deprecation",
    "kafka had deprecated this method but it still allow developer to override it. We forbrid the inherance now"
  })
  @Override
  public final void onPartitionsRevoked(
      Collection<org.apache.kafka.common.TopicPartition> partitions) {
    super.onPartitionsRevoked(partitions);
  }

  @Override
  public final void flush(
      Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> currentOffsets) {
    // this API in connector is embarrassing since it is a part from default implementation from
    // preCommit...
  }
}
