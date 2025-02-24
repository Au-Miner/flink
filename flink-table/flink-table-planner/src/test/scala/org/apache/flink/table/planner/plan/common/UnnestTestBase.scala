/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.common

import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}
import org.apache.flink.table.types.AbstractDataType

import org.junit.jupiter.api.Test

import java.sql.Timestamp

/** Test for UNNEST queries. */
abstract class UnnestTestBase(withExecPlan: Boolean) extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil

  protected def getTableTestUtil: TableTestUtil

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    util.addTableSource[(Int, Array[Int], Array[Array[Int]])]("MyTable", 'a, 'b, 'c)
    verifyPlan("SELECT a, b, s FROM MyTable, UNNEST(MyTable.b) AS A (s)")
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {
    util.addTableSource[(Int, Array[Int], Array[Array[Int]])]("MyTable", 'a, 'b, 'c)
    verifyPlan("SELECT a, s FROM MyTable, UNNEST(MyTable.c) AS A (s)")
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    verifyPlan("SELECT a, b, s, t FROM MyTable, UNNEST(MyTable.b) AS A (s, t) WHERE s > 13")
  }

  @Test
  def testUnnestMultiSetFromCollectResult(): Unit = {
    util.addDataStream[(Int, Int, (Int, String))]("MyTable", 'a, 'b, 'c)
    val sqlQuery =
      """
        |WITH T AS (SELECT b, COLLECT(c) as `set` FROM MyTable GROUP BY b)
        |SELECT b, id, point FROM T, UNNEST(T.`set`) AS A(id, point) WHERE b < 3
      """.stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    util.addDataStream[(Int, String, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery =
      """
        |WITH T AS (SELECT a, COLLECT(b) as `set` FROM MyTable GROUP BY a)
        |SELECT a, s FROM T LEFT JOIN UNNEST(T.`set`) AS A(s) ON TRUE WHERE a < 5
      """.stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    util.addDataStream[(Int, Long, String, Timestamp)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val sqlQuery =
      """
        |WITH T AS (SELECT b, COLLECT(b) as `set`
        |    FROM MyTable
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '3' SECOND)
        |)
        |SELECT b, s FROM T, UNNEST(T.`set`) AS A(s) where b < 3
      """.stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testCrossWithUnnest(): Unit = {
    util.addTableSource[(Int, Long, Array[String])]("MyTable", 'a, 'b, 'c)
    verifyPlan("SELECT a, s FROM MyTable, UNNEST(MyTable.c) as A (s)")
  }

  @Test
  def testUnnestWithValues(): Unit = {
    verifyPlan("SELECT * FROM UNNEST(ARRAY[1,2,3])")
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    util.addTableSource(
      "MyTable",
      Array[AbstractDataType[_]](
        DataTypes.INT,
        DataTypes.BIGINT,
        DataTypes.MAP(DataTypes.STRING, DataTypes.STRING)),
      Array("a", "b", "c"))
    verifyPlan("SELECT a, b, v FROM MyTable CROSS JOIN UNNEST(c) as f(k, v)")
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    val sqlQuery =
      """
        |SELECT a, b, x, y FROM
        |    (SELECT a, b FROM MyTable WHERE a < 3) as tf,
        |    UNNEST(tf.b) as A (x, y)
        |WHERE x > a
      """.stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testUnnestObjectArrayWithoutAlias(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    verifyPlan("SELECT a, b, A._1, A._2 FROM MyTable, UNNEST(MyTable.b) AS A where A._1 > 1")
  }

  @Test
  def testUnnestWithNestedFilter(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    val sqlQuery =
      """
        |SELECT * FROM (
        |   SELECT a, b1, b2 FROM
        |       (SELECT a, b FROM MyTable) T
        |       CROSS JOIN
        |       UNNEST(T.b) AS S(b1, b2)
        |       WHERE S.b1 >= 12
        |   ) tmp
        |WHERE b2 <> 'Hello'
    """.stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testUnnestWithOrdinalityAndValues(): Unit = {
    verifyPlan("SELECT val, pos FROM UNNEST(ARRAY[1,2,3]) WITH ORDINALITY AS t(val, pos)")
  }

  @Test
  def testUnnestWithOrdinalityArray(): Unit = {
    util.addTableSource[(Int, Array[Int])]("MyTable", 'a, 'b)
    verifyPlan(
      "SELECT a, number, ordinality FROM MyTable CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)")
  }

  @Test
  def testUnnestWithOrdinalityArrayOfRowsWithoutAlias(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    verifyPlan(
      "SELECT a, b, A._1, A._2, A.`ORDINALITY` FROM MyTable, UNNEST(MyTable.b) WITH ORDINALITY AS A where A._1 > 1")
  }

  @Test
  def testUnnestWithOrdinalityArrayOfRowsFromTableWithFilter(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    verifyPlan(
      "SELECT a, b, s, t, o FROM MyTable, UNNEST(MyTable.b) WITH ORDINALITY AS A (s, t, o) WHERE s > 13")
  }

  @Test
  def testUnnestWithOrdinalityArrayOfArray(): Unit = {
    util.addTableSource[(Int, Array[Array[Int]])]("MyTable", 'id, 'nested_array)
    val sqlQuery =
      """
        |SELECT id, array_val, array_pos, elem, element_pos
        |FROM MyTable
        |CROSS JOIN UNNEST(nested_array) WITH ORDINALITY AS A(array_val, array_pos)
        |CROSS JOIN UNNEST(array_val) WITH ORDINALITY AS B(elem, element_pos)
        |""".stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testUnnestWithOrdinalityMultiset(): Unit = {
    util.addDataStream[(Int, String, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery =
      """
        |WITH T AS (SELECT a, COLLECT(c) as words FROM MyTable GROUP BY a)
        |SELECT a, word, pos
        |FROM T CROSS JOIN UNNEST(words) WITH ORDINALITY AS A(word, pos)
        |""".stripMargin
    verifyPlan(sqlQuery)
  }

  @Test
  def testUnnestWithOrdinalityMap(): Unit = {
    util.addTableSource(
      "MyTable",
      Array[AbstractDataType[_]](
        DataTypes.INT(),
        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
      Array("id", "map_data"))
    verifyPlan(
      "SELECT id, k, v, pos FROM MyTable CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS f(k, v, pos)")
  }

  @Test
  def testUnnestWithOrdinalityWithFilter(): Unit = {
    util.addTableSource[(Int, Array[Int])]("MyTable", 'a, 'b)
    verifyPlan("""
                 |SELECT a, number, ordinality 
                 |FROM MyTable 
                 |CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
                 |WHERE number > 10 AND ordinality < 3
                 |""".stripMargin)
  }

  @Test
  def testUnnestWithOrdinalityInSubquery(): Unit = {
    util.addTableSource[(Int, Array[Int])]("MyTable", 'a, 'b)
    verifyPlan("""
                 |SELECT * FROM (
                 |  SELECT a, number, ordinality 
                 |  FROM MyTable 
                 |  CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
                 |) subquery 
                 |WHERE ordinality = 1
                 |""".stripMargin)
  }

  def verifyPlan(sql: String): Unit = {
    if (withExecPlan) {
      util.verifyExecPlan(sql)
    } else {
      util.verifyRelPlan(sql)
    }
  }
}
