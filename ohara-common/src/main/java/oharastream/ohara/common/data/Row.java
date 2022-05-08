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

package oharastream.ohara.common.data;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.IntBinaryOperator;
import java.util.stream.Collectors;

/**
 * a collection from {@link Cell}. Also, {@link Row} can carry variable tags which can be used to
 * add more "description" to the {@link Row}. NOTED. the default implementation from {@link Row} has
 * implemented the
 */
public interface Row extends Iterable<Cell<?>> {
  /** a empty row. It means no tag and cell exist in this row. */
  Row EMPTY = Row.of();

  /** @return a collection from name from cells */
  List<String> names();

  /**
   * seek the cell by specified index
   *
   * @param index cell's index
   * @return a cell or throw NoSuchElementException
   */
  Cell<?> cell(int index);

  /**
   * seek the cell by specified name
   *
   * @param name cell's name
   * @return a cell or throw NoSuchElementException
   */
  Cell<?> cell(String name);

  /** @return a immutable collection from cells */
  List<Cell<?>> cells();

  /**
   * the default order from cells from this method is same to {@link #cells()}
   *
   * @return an iterator over the {@link Cell} in this row in proper sequence
   */
  @Override
  default Iterator<Cell<?>> iterator() {
    List<Cell<?>> cells = cells();
    if (cells == null) cells = List.of();
    return cells.iterator();
  }

  /** @return a immutable collection from tags */
  List<String> tags();

  /** @return the number from elements in this row */
  default int size() {
    return cells().size();
  }

  /**
   * Compare all cells one-by-one. Noted: the order from cells doesn't impact the comparison.
   *
   * @param that another row
   * @param includeTags true if the tags should be considered in the comparison
   * @return true if both rows have same cells and tags (if includeTags is true)
   */
  default boolean equals(Row that, boolean includeTags) {
    if (cells().size() != that.cells().size()) return false;
    if (includeTags && !tags().stream().allMatch(tag -> that.tags().stream().anyMatch(tag::equals)))
      return false;
    return cells().stream().allMatch(cell -> that.cells().stream().anyMatch(cell::equals));
  }

  static Row of(Cell<?>... cells) {
    return of(List.of(), cells);
  }

  static Row of(List<String> tags, Cell<?>... cells) {
    var tagsCopy = List.copyOf(tags);
    var cellsCopy = List.of(cells);
    // check duplicate names
    int numberOfNames =
        cellsCopy.stream().map(Cell::name).collect(Collectors.toUnmodifiableSet()).size();
    if (numberOfNames != cellsCopy.size())
      throw new IllegalArgumentException("Row can't accept duplicate cell name");
    return new Row() {

      @Override
      public List<String> names() {
        return cellsCopy.stream().map(Cell::name).collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Cell<?> cell(int index) {
        Cell<?> cell = cellsCopy.get(index);
        if (cell == null) throw new NoSuchElementException("no cell exists with index:" + index);
        return cell;
      }

      @Override
      public Cell<?> cell(String name) {
        return cellsCopy.stream()
            .filter(c -> c.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException("no cell exists with name:" + name));
      }

      @Override
      public List<Cell<?>> cells() {
        return cellsCopy;
      }

      @Override
      public List<String> tags() {
        return tagsCopy;
      }

      @Override
      public int hashCode() {
        IntBinaryOperator accumulate = (hash, current) -> hash * 31 + current;
        return 31 * cells().stream().mapToInt(Objects::hashCode).reduce(1, accumulate)
            + tags.stream().mapToInt(Objects::hashCode).reduce(1, accumulate);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Row) return equals((Row) obj, true);
        return false;
      }

      @Override
      public String toString() {
        return "cells:" + cells() + ", tags:" + tags;
      }
    };
  }
}
