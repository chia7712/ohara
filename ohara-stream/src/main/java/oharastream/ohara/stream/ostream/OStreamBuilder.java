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

import java.util.Objects;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.pattern.Builder;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.stream.OStream;

/**
 * This class is responsible for managing all the properties that will use in {@code OStream}. Use
 * this class to construct {@code OStream} only.
 */
@SuppressWarnings({"rawtypes"})
public final class OStreamBuilder implements Builder<OStream<Row>> {

  private ObjectKey key = null;
  private String bootstrapServers = null;
  private String fromTopic = null;
  private String toTopic = null;
  private Class<? extends TimestampExtractor> extractor = null;
  private boolean cleanStart = false;
  private boolean exactlyOnce = false;

  // We are in Ohara world, the data type of topics must be <Row, byte[]> for current version...
  private final Consumed fromSerde = new Consumed<>(Serdes.ROW, Serdes.BYTES);
  private final Produced toSerde = new Produced<>(Serdes.ROW, Serdes.BYTES);

  public static OStreamBuilder builder() {
    return new OStreamBuilder();
  }

  private OStreamBuilder() {}

  public OStreamBuilder key(ObjectKey key) {
    this.key = Objects.requireNonNull(key);
    return this;
  }

  /**
   * @param bootstrapServers broker list
   * @return this builder
   */
  public OStreamBuilder bootstrapServers(String bootstrapServers) {
    this.bootstrapServers = CommonUtils.requireNonEmpty(bootstrapServers);
    return this;
  }

  /**
   * set the topic consumed from.
   *
   * @param fromTopic the topic name
   * @return this builder
   */
  public OStreamBuilder fromTopic(String fromTopic) {
    this.fromTopic = CommonUtils.requireNonEmpty(fromTopic);
    return this;
  }

  /**
   * set the topic produced to.
   *
   * @param toTopic the topic name
   * @return this builder
   */
  public OStreamBuilder toTopic(String toTopic) {
    this.toTopic = CommonUtils.requireNonEmpty(toTopic);
    return this;
  }

  /**
   * enable exactly once. Note: This method is intend to test the functionality for current version.
   * Since we will have a better way to passing the "configurable" properties for cluster settings.
   *
   * @return this builder
   */
  @VisibleForTesting
  OStreamBuilder enableExactlyOnce() {
    this.exactlyOnce = true;
    return this;
  }

  /**
   * control this stream application should clean all state data before start. Note: This method is
   * intend to test the functionality for current version. Since we will have a better way to
   * passing the "configurable" properties for cluster settings.
   *
   * @return this builder
   */
  @VisibleForTesting
  OStreamBuilder cleanStart() {
    this.cleanStart = true;
    return this;
  }

  /**
   * define timestamp of fromTopic records. Note: This method is intend to test the functionality
   * for current version. Since we will have a better way to passing the "configurable" properties
   * for cluster settings.
   *
   * @param extractor class extends {@code TimestampExtractor}
   * @return this builder
   */
  @VisibleForTesting
  OStreamBuilder timestampExtractor(Class<? extends TimestampExtractor> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    return this;
  }

  private void checkArguments() {
    Objects.requireNonNull(key);
    CommonUtils.requireNonEmpty(bootstrapServers);
    CommonUtils.requireNonEmpty(fromTopic);
    CommonUtils.requireNonEmpty(toTopic);
  }

  @Override
  public OStream<Row> build() {
    checkArguments();
    return new OStreamImpl(this);
  }

  // Getters
  ObjectKey key() {
    return Objects.requireNonNull(key);
  }

  String getBootstrapServers() {
    return bootstrapServers;
  }

  String getAppId() {
    return Objects.requireNonNull(key).toPlain();
  }

  String getFromTopic() {
    return fromTopic;
  }

  Consumed getFromSerde() {
    return fromSerde;
  }

  String getToTopic() {
    return toTopic;
  }

  Produced getToSerde() {
    return toSerde;
  }

  Class<? extends TimestampExtractor> getExtractor() {
    return extractor;
  }

  boolean isCleanStart() {
    return cleanStart;
  }

  boolean getExactlyOnce() {
    return exactlyOnce;
  }
}
