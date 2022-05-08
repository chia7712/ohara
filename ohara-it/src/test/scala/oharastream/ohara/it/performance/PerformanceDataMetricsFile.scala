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

package oharastream.ohara.it.performance

import java.io.{File, FileWriter}
import oharastream.ohara.common.util.{CommonUtils, Releasable}

class PerformanceDataMetricsFile {
  private[this] val logDataInfoFileName = s"${CommonUtils.randomString(5)}.csv"

  private[this] val reportOutputFolderKey = PerformanceTestingUtils.REPORT_OUTPUT_KEY
  private[this] val reportOutputFolder: File = mkdir(
    new File(
      value(reportOutputFolderKey).getOrElse("/tmp/performance")
    )
  )

  def logMeters(report: PerformanceReport): Unit = if (report.records.nonEmpty) {
    // we have to fix the order of key-value
    // if we generate line via map.keys and map.values, the order may be different ...
    val headers = report.records.head._2.keySet.toList
    report.records.values.foreach { items =>
      headers.foreach(
        name =>
          if (!items.contains(name))
            throw new RuntimeException(s"$name disappear?? current:${items.keySet.mkString(",")}")
      )
    }
    val file = path(report.className, s"${report.key.group()}-${report.key.name()}.csv")
    if (file.exists() && !file.delete()) throw new RuntimeException(s"failed to remove file:$file")
    val fileWriter = new FileWriter(file)
    try {
      fileWriter.write("duration," + headers.map(s => s"""\"$s\"""").mkString(","))
      fileWriter.write("\n")
      report.records.foreach {
        case (duration, item) =>
          val line = s"$duration," + headers.map(header => f"${item(header)}%.3f").mkString(",")
          fileWriter.write(line)
          fileWriter.write("\n")
      }
    } finally Releasable.close(fileWriter)
  }

  def logDataInfos(inputDataInfos: Seq[DataInfo]): Unit = {
    val file = path("inputdata", logDataInfoFileName)
    if (file.exists() && !file.delete()) throw new RuntimeException(s"failed to remove file:$file")
    val fileWriter = new FileWriter(file)
    try {
      fileWriter.write("duration,messageNumber,messageSize\n")
      inputDataInfos
        .sortBy(_.duration)(Ordering[Long].reverse)
        .foreach(inputDataInfo => {
          fileWriter.write(
            s"${inputDataInfo.duration / 1000},${inputDataInfo.messageNumber},${inputDataInfo.messageSize}\n"
          )
        })
    } finally Releasable.close(fileWriter)
  }

  private[this] def simpleName(className: String): String = {
    val index = className.lastIndexOf(".")
    if (index != -1) className.substring(index + 1)
    else className
  }

  private[this] def path(className: String, fileName: String): File = {
    new File(
      mkdir(new File(mkdir(new File(reportOutputFolder, simpleName(className))), this.getClass.getSimpleName)),
      fileName
    )
  }

  private[this] def mkdir(folder: File): File = {
    if (!folder.exists() && !folder.mkdirs()) throw new AssertionError(s"failed to create folder on $folder")
    if (folder.exists() && !folder.isDirectory) throw new AssertionError(s"$folder is not a folder")
    folder
  }

  private[this] def value(key: String): Option[String] = sys.env.get(key)
}
