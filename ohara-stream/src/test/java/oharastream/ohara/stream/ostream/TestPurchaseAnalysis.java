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

package oharastream.ohara.stream.ostream;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.Consumer;
import oharastream.ohara.kafka.Producer;
import oharastream.ohara.kafka.TopicAdmin;
import oharastream.ohara.stream.OStream;
import oharastream.ohara.stream.Stream;
import oharastream.ohara.stream.config.StreamDefUtils;
import oharastream.ohara.stream.config.StreamSetting;
import oharastream.ohara.testing.With3Brokers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes"})
public class TestPurchaseAnalysis extends With3Brokers {
  private static final Logger LOG = LoggerFactory.getLogger(TestPurchaseAnalysis.class);
  private static final TopicKey resultTopic =
      TopicKey.of(CommonUtils.randomString(), "gender-amount");
  private static final TopicKey itemTopic = TopicKey.of(CommonUtils.randomString(), "items");
  private static final TopicKey orderTopic = TopicKey.of(CommonUtils.randomString(), "orders");
  private static final TopicKey userTopic = TopicKey.of(CommonUtils.randomString(), "users");
  private final TopicAdmin client = TopicAdmin.of(testUtil().brokersConnProps());
  private final Producer<Row, byte[]> producer =
      Producer.builder()
          .connectionProps(client.connectionProps())
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build();

  @BeforeEach
  public void setup() {
    int partitions = 3;
    short replications = 1;
    try {
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicKey(orderTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicKey(itemTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicKey(userTopic)
          .create();
      client
          .topicCreator()
          .numberOfPartitions(partitions)
          .numberOfReplications(replications)
          .topicKey(resultTopic)
          .create();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Test
  public void testStream() throws InterruptedException {
    // write items.csv to kafka broker
    produceData("items.csv", itemTopic);

    // write users.csv to kafka broker
    produceData("users.csv", userTopic);

    // we make sure the join topic has data already
    assertResult(client, itemTopic, 4);
    assertResult(client, userTopic, 4);
    TimeUnit.SECONDS.sleep(1);
    // write orders.csv to kafka broker
    produceData("orders.csv", orderTopic);
    assertResult(client, orderTopic, 16);

    RunStream app = new RunStream();
    Stream.execute(
        app.getClass(),
        Map.of(
            StreamDefUtils.GROUP_DEFINITION.key(), CommonUtils.randomString(5),
            StreamDefUtils.NAME_DEFINITION.key(), "test-purchase-analysis",
            StreamDefUtils.BROKER_DEFINITION.key(), client.connectionProps(),
            StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key(),
                TopicKey.toJsonString(List.of(orderTopic)),
            StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key(),
                TopicKey.toJsonString(List.of(resultTopic))));
  }

  @AfterEach
  public void cleanUp() {
    producer.close();
    client.close();
  }

  /** Stream Main Entry */
  public static class RunStream extends Stream {

    @Override
    public void start(OStream<Row> stream, StreamSetting streamSetting) {
      // We initial a new OStream object to test functionality
      OStream<Row> ostream =
          OStream.builder()
              .key(streamSetting.key())
              .bootstrapServers(streamSetting.brokerConnectionProps())
              .fromTopic(
                  streamSetting.fromTopicKeys().stream()
                      .map(TopicKey::topicNameOnKafka)
                      .findFirst()
                      .orElse(null))
              .toTopic(
                  streamSetting.toTopicKeys().stream()
                      .map(TopicKey::topicNameOnKafka)
                      .findFirst()
                      .orElse(null))
              .cleanStart()
              .timestampExtractor(MyExtractor.class)
              .enableExactlyOnce()
              .build();

      ostream
          .leftJoin(
              userTopic.topicNameOnKafka(),
              Conditions.create().add(List.of(Map.entry("userName", "name"))),
              (row1, row2) ->
                  Row.of(
                      row1.cell("userName"),
                      row1.cell("itemName"),
                      row1.cell("quantity"),
                      row2 == null ? Cell.of("address", "") : row2.cell("address"),
                      row2 == null ? Cell.of("gender", "") : row2.cell("gender")))
          .filter(row -> row.cell("address").value() != null)
          .leftJoin(
              itemTopic.topicNameOnKafka(),
              Conditions.create().add(List.of(Map.entry("itemName", "itemName"))),
              (row1, row2) ->
                  Row.of(
                      row1.cell("userName"),
                      row1.cell("itemName"),
                      row1.cell("quantity"),
                      Cell.of("useraddress", row1.cell("address").value()),
                      row1.cell("gender"),
                      row2 == null
                          ? Cell.of("itemaddress", "")
                          : Cell.of("itemaddress", row2.cell("address").value()),
                      row2 == null ? Cell.of("type", "") : row2.cell("type"),
                      row2 == null ? Cell.of("price", "") : row2.cell("price")))
          .filter(
              row ->
                  row.cell("useraddress")
                      .value()
                      .toString()
                      .equals(row.cell("itemaddress").value().toString()))
          .map(
              row ->
                  Row.of(
                      row.cell("gender"),
                      Cell.of(
                          "amount",
                          Double.parseDouble(row.cell("quantity").value().toString())
                              * Double.parseDouble(row.cell("price").value().toString()))))
          .groupByKey(List.of("gender"))
          .reduce(Double::sum, "amount")
          .start();

      Consumer<Row, byte[]> consumer =
          Consumer.builder()
              .topicKey(resultTopic)
              .connectionProps(streamSetting.brokerConnectionProps())
              .groupId("group-" + resultTopic.topicNameOnKafka())
              .offsetFromBegin()
              .keySerializer(Serializer.ROW)
              .valueSerializer(Serializer.BYTES)
              .build();

      List<Consumer.Record<Row, byte[]>> records = consumer.poll(Duration.ofSeconds(30), 4);
      records.forEach(
          row ->
              LOG.debug(
                  "final result : " + (row.key().isPresent() ? row.key().get().toString() : null)));
      Assertions.assertEquals(4, records.size());

      Map<String, Double[]> actualResultMap =
          Map.of(
              "male", new Double[] {9000D, 60000D, 69000D},
              "female", new Double[] {15000D, 30000D, 45000D});
      final double THRESHOLD = 0.0001;

      records.forEach(
          record -> {
            if (record.key().isPresent()) {
              Optional<Double> amount =
                  record.key().get().cells().stream()
                      .filter(cell -> cell.name().equals("amount"))
                      .map(cell -> Double.valueOf(cell.value().toString()))
                      .findFirst();
              Assertions.assertTrue(
                  actualResultMap.containsKey(record.key().get().cell("gender").value().toString())
                      && actualResultMap.values().stream()
                          .flatMap(Arrays::stream)
                          .anyMatch(d -> Math.abs(d - amount.orElse(-999.0)) < THRESHOLD));
            }
          });

      consumer.close();
      ostream.stop();
    }
  }

  public static class MyExtractor implements TimestampExtractor {

    @Override
    public long extract(
        org.apache.kafka.clients.consumer.ConsumerRecord<Object, Object> record,
        long previousTimestamp) {
      LOG.debug(
          String.format(
              "timeExtract : topic[%s], value[%s], partition[%s], time[%s]",
              record.topic(), record.key().toString(), record.partition(), record.timestamp()));
      Object value = record.key();
      if (value instanceof Row) {
        Row row = (Row) value;
        // orders
        if (row.names().contains("transactionDate"))
          return LocalDateTime.of(2019, 2, 2, 2, 2, 2).toEpochSecond(ZoneOffset.UTC) * 1000;
        // items
        else if (row.names().contains("price"))
          return LocalDateTime.of(2019, 1, 1, 1, 1, 1).toEpochSecond(ZoneOffset.UTC) * 1000;
        // users
        else if (row.names().contains("gender"))
          return LocalDateTime.of(2019, 1, 1, 1, 1, 1).toEpochSecond(ZoneOffset.UTC) * 1000;
        // other
        else
          throw new RuntimeException(
              "the headers of this row are not expected :" + String.join(",", row.names()));
      } else {
        throw new RuntimeException("who are you? :" + value.getClass().getName());
      }
    }
  }

  private void produceData(String filename, TopicKey topicKey) {
    try {
      List<?> dataList = DataUtils.readData(filename);
      dataList.stream()
          .map(
              object -> {
                try {
                  List<Cell> cells = new ArrayList<>();
                  LOG.debug("Class Name : " + object.getClass().getName());
                  for (Field f : object.getClass().getDeclaredFields()) {
                    f.setAccessible(true);
                    Cell cell = Cell.of(f.getName(), f.get(object));
                    cells.add(cell);
                    LOG.debug("--" + f.getName() + ":" + f.get(object));
                  }
                  return new AbstractMap.SimpleEntry<>(
                      Row.of(cells.toArray(new Cell[0])), new byte[0]);
                } catch (Exception e) {
                  LOG.debug(e.getMessage());
                  return new AbstractMap.SimpleEntry<>(Row.EMPTY, new byte[0]);
                }
              })
          .forEach(
              entry ->
                  producer
                      .sender()
                      .key(entry.getKey())
                      .value(entry.getValue())
                      .topicKey(topicKey)
                      .send());
    } catch (Exception e) {
      LOG.debug(e.getMessage());
    }
  }

  private void assertResult(TopicAdmin client, TopicKey topicKey, int expectedSize) {
    Consumer<Row, byte[]> consumer =
        Consumer.builder()
            .topicKey(topicKey)
            .connectionProps(client.connectionProps())
            .groupId("group-" + CommonUtils.randomString(5))
            .offsetFromBegin()
            .keySerializer(Serializer.ROW)
            .valueSerializer(Serializer.BYTES)
            .build();

    List<Consumer.Record<Row, byte[]>> records =
        consumer.poll(Duration.ofSeconds(30), expectedSize);
    Assertions.assertEquals(expectedSize, records.size());
    consumer.close();
  }
}
