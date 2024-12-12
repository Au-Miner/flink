package org.apache.flink.table.runtime.operators.aggregate.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;

public abstract class MiniBatchGroupAggHelper {

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Reused output row. */
    private transient JoinedRowData resultRow = new JoinedRowData();

    // function used to handle all aggregates
    private transient AggsHandleFunction function = null;

    public MiniBatchGroupAggHelper(
            RecordCounter recordCounter, JoinedRowData resultRow, AggsHandleFunction function) {
        this.recordCounter = recordCounter;
        this.resultRow = resultRow;
        this.function = function;
    }

    public void processAggregate(
            RowData acc, List<RowData> inputRows, RowData currentKey, Collector<RowData> out)
            throws Exception {
        boolean firstRow = false;
        if (acc == null) {
            // Don't create a new accumulator for a retraction message. This
            // might happen if the retraction message is the first message for the
            // key or after a state clean up.
            Iterator<RowData> inputIter = inputRows.iterator();
            while (inputIter.hasNext()) {
                RowData current = inputIter.next();
                if (isRetractMsg(current)) {
                    inputIter.remove(); // remove all the beginning retraction messages
                } else {
                    break;
                }
            }
            if (inputRows.isEmpty()) {
                return;
            }
            acc = function.createAccumulators();
            firstRow = true;
        }

        // step 2: accumulate
        function.setAccumulators(acc);

        // get previous aggregate result
        RowData prevAggValue = function.getValue();

        for (RowData input : inputRows) {
            if (isAccumulateMsg(input)) {
                function.accumulate(input);
            } else {
                function.retract(input);
            }
        }

        // get current aggregate result
        RowData newAggValue = function.getValue();

        // get updated accumulator
        acc = function.getAccumulators();

        // TODO 检查一下recordCounter这个有没有async版本
        if (!recordCounter.recordCountIsZero(acc)) {
            updateStateAndEmitResult(acc, firstRow, prevAggValue, newAggValue, currentKey, out);
        } else {
            // we retracted the last record for this key
            // if this is not first row sent out a DELETE message
            if (!firstRow) {
                // prepare DELETE message for previous row
                resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
                out.collect(resultRow);
            }
            // and clear all state
            clearAccumulatorsState();
            // cleanup dataview under current key
            function.cleanup();
        }
    }

    protected abstract void clearAccumulatorsState() throws Exception;

    protected abstract void updateStateAndEmitResult(
            RowData acc,
            boolean firstRow,
            RowData prevAggValue,
            RowData newAggValue,
            RowData currentKey,
            Collector<RowData> out)
            throws Exception;
}
