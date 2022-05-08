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

package oharastream.ohara.stream.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;

/**
 * The {@code Poneglyph} represents the overall logic flow in this stream. User can recognize the
 * from-and-to view in each operation {@link Stele}. A {@code Poneglyph} is represent a complete
 * flow from data in to data out, there may have multiple {@code Poneglyph} in a stream.
 */
public final class Poneglyph implements Serializable {
  private static final long serialVersionUID = 1L;
  private final HashSet<Stele> steles = new HashSet<>();

  public void addStele(Stele another) {
    this.steles.add(another);
  }

  public HashSet<Stele> getSteles() {
    return this.steles;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Poneglyph poneglyph = (Poneglyph) o;
    return Objects.equals(steles, poneglyph.steles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(steles);
  }

  @Override
  public String toString() {
    return "Poneglyph{" + "steles=" + steles + '}';
  }
}
