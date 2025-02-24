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

package org.apache.flink.table.planner.plan.nodes.exec.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.TestTemplate;

import java.util.Optional;

/** Tests for verifying name and description of batch sql operator. */
class BatchOperatorNameTest extends OperatorNameTestBase {

    @Override
    protected TableTestUtil getTableTestUtil() {
        return batchTestUtil(TableConfig.getDefault());
    }

    @TestTemplate
    void testBoundedStreamScan() {
        final DataStream<Integer> dataStream = util.getStreamEnv().fromElements(1, 2, 3, 4, 5);
        TableTestUtil.createTemporaryView(
                tEnv,
                "MyTable",
                dataStream,
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()));
        verifyQuery("SELECT * FROM MyTable");
    }

    /** Verify Expand, HashAggregate. */
    @TestTemplate
    void testHashAggregate() {
        createTestSource();
        verifyQuery("SELECT a, " + "count(distinct b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify Sort, SortAggregate. */
    @TestTemplate
    void testSortAggregate() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");
        createTestSource();
        verifyQuery("SELECT a, " + "count(distinct b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify SortWindowAggregate. */
    @TestTemplate
    void testSortWindowAggregate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT\n"
                        + "  b,\n"
                        + "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as window_end,\n"
                        + "  FIRST_VALUE(a)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, TUMBLE(rowtime, INTERVAL '15' MINUTE)");
    }

    /** Verify HashJoin. */
    @TestTemplate
    void testHashJoin() {
        testJoinInternal();
    }

    /** Verify NestedLoopJoin. */
    @TestTemplate
    void testNestedLoopJoin() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, SortMergeJoin");
        testJoinInternal();
    }

    /** Verify SortMergeJoin. */
    @TestTemplate
    void testSortMergeJoin() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, NestedLoopJoin");
        testJoinInternal();
    }

    /** Verify MultiInput. */
    @TestTemplate
    void testMultiInput() {
        createTestSource("A");
        createTestSource("B");
        createTestSource("C");
        verifyQuery("SELECT * FROM A, B, C where A.a = B.a and A.a = C.a");
    }

    /** Verify Limit. */
    @TestTemplate
    void testLimit() {
        createTestSource();
        verifyQuery("select * from MyTable limit 10");
    }

    /** Verify SortLimit. */
    @TestTemplate
    void testSortLimit() {
        createTestSource();
        verifyQuery("select * from MyTable order by a limit 10");
    }

    @TestTemplate
    void testMatch() {
        createSourceWithTimeAttribute();
        String sql =
                "SELECT T.aid, T.bid, T.cid\n"
                        + "     FROM MyTable MATCH_RECOGNIZE (\n"
                        + "             ORDER BY proctime\n"
                        + "             MEASURES\n"
                        + "             `A\"`.a AS aid,\n"
                        + "             \u006C.a AS bid,\n"
                        + "             C.a AS cid\n"
                        + "             PATTERN (`A\"` \u006C C)\n"
                        + "             DEFINE\n"
                        + "                 `A\"` AS a = 1,\n"
                        + "                 \u006C AS b = 2,\n"
                        + "                 C AS c = 'c'\n"
                        + "     ) AS T";
        verifyQuery(sql);
    }
}
