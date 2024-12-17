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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.AssociatedRecords;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.utils.SemiAntiJoinHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/** Streaming unbounded Join operator which supports SEMI/ANTI JOIN. */
public class StreamingSemiAntiJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -3135772379944924519L;

    // true if it is anti join, otherwise is semi join
    private final boolean isAntiJoin;

    // left join state
    private transient OuterJoinRecordStateView leftRecordStateView;
    // right join state
    private transient JoinRecordStateView rightRecordStateView;

    private transient SyncStateSemiAntiJoinHelper semiAntiJoinHelper;

    public StreamingSemiAntiJoinOperator(
            boolean isAntiJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTIme) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTIme);
        this.isAntiJoin = isAntiJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.leftRecordStateView =
                OuterJoinRecordStateViews.create(
                        getRuntimeContext(),
                        LEFT_RECORDS_STATE_NAME,
                        leftInputSideSpec,
                        leftType,
                        leftStateRetentionTime);

        this.rightRecordStateView =
                JoinRecordStateViews.create(
                        getRuntimeContext(),
                        RIGHT_RECORDS_STATE_NAME,
                        rightInputSideSpec,
                        rightType,
                        rightStateRetentionTime);
        this.semiAntiJoinHelper = new SyncStateSemiAntiJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        AssociatedRecords associatedRecords =
                AssociatedRecords.fromSyncStateView(
                        input, true, rightRecordStateView, joinCondition);
        semiAntiJoinHelper.processLeftJoin(associatedRecords, input, leftRecordStateView);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        AssociatedRecords associatedRecords =
                AssociatedRecords.fromSyncStateView(
                        input, false, leftRecordStateView, joinCondition);
        semiAntiJoinHelper.processRightJoin(
                isAccumulateMsg,
                leftRecordStateView,
                rightRecordStateView,
                input,
                associatedRecords,
                inputRowKind);
    }

    private class SyncStateSemiAntiJoinHelper
            extends SemiAntiJoinHelper<JoinRecordStateView, OuterJoinRecordStateView> {

        public SyncStateSemiAntiJoinHelper() {
            super(isAntiJoin, collector);
        }

        @Override
        public void addRecord(JoinRecordStateView stateView, RowData record) throws Exception {
            // no need to wait for the future
            stateView.addRecord(record);
        }

        @Override
        public void retractRecord(JoinRecordStateView stateView, RowData record) throws Exception {
            // no need to wait for the future
            stateView.retractRecord(record);
        }

        @Override
        public void addRecordInOuterSide(
                OuterJoinRecordStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            // no need to wait for the future
            stateView.addRecord(record, numOfAssociations);
        }

        @Override
        public void retractRecordInOuterSide(OuterJoinRecordStateView stateView, RowData record)
                throws Exception {
            // no need to wait for the future
            stateView.retractRecord(record);
        }

        @Override
        public void updateNumOfAssociationsInOuterSide(
                OuterJoinRecordStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            // no need to wait for the future
            stateView.updateNumOfAssociations(record, numOfAssociations);
        }
    }
}
