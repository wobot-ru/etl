package ru.wobot.etl

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

/**
  * Implements the "WordCount" program that computes a simple word occurrence histogram
  * over some sample data
  *
  * This example shows how to:
  *
  * - write a simple Flink program.
  * - use Tuple data types.
  * - write and use user-defined functions.
  */
object SegmentExporter {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val job = org.apache.hadoop.mapreduce.Job.getInstance()
    val format = new HadoopInputFormat[Text, Writable](new SequenceFileInputFormat[Text, Writable], classOf[Text], classOf[Writable], job)
    val input = env.createInput(format)
    //val count = groupBy.count();

  }
}
