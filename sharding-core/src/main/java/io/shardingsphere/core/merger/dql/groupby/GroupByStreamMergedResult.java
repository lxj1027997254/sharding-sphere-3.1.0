/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
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
 * </p>
 */

package io.shardingsphere.core.merger.dql.groupby;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.shardingsphere.core.merger.QueryResult;
import io.shardingsphere.core.merger.dql.groupby.aggregation.AggregationUnit;
import io.shardingsphere.core.merger.dql.groupby.aggregation.AggregationUnitFactory;
import io.shardingsphere.core.merger.dql.orderby.OrderByStreamMergedResult;
import io.shardingsphere.core.parsing.parser.context.selectitem.AggregationSelectItem;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Stream merged result for group by.
 *
 * @author zhangliang
 */
public final class GroupByStreamMergedResult extends OrderByStreamMergedResult {
    
    private final Map<String, Integer> labelAndIndexMap;
    
    private final SelectStatement selectStatement;
    
    private final List<Object> currentRow;
    
    private List<?> currentGroupByValues;

    // select name, sum(age) from user group by name
    public GroupByStreamMergedResult(
            final Map<String, Integer> labelAndIndexMap, final List<QueryResult> queryResults, final SelectStatement selectStatement) throws SQLException {
        super(queryResults, selectStatement.getOrderByItems());
        // 列名和索引的关系
        this.labelAndIndexMap = labelAndIndexMap;
        this.selectStatement = selectStatement;
        // 调用一次next()后聚合项值累加的结果（例如name="a"时的sum(age)）
        currentRow = new ArrayList<>(labelAndIndexMap.size());
        // 队首分组项的值（例如name="a"）
        currentGroupByValues = getOrderByValuesQueue().isEmpty() ? Collections.emptyList() : new GroupByValue(getCurrentQueryResult(), selectStatement.getGroupByItems()).getGroupValues();
    }
    
    @Override
    public boolean next() throws SQLException {
        currentRow.clear();
        if (getOrderByValuesQueue().isEmpty()) {
            return false;
        }
        if (isFirstNext()) {
            super.next();
        }
        // 聚合当前分组项的值（如sum(age)）
        if (aggregateCurrentGroupByRowAndNext()) {
            // 设置新的分组项（如name="b"）
            currentGroupByValues = new GroupByValue(getCurrentQueryResult(), selectStatement.getGroupByItems()).getGroupValues();
        }
        return true;
    }
    
    private boolean aggregateCurrentGroupByRowAndNext() throws SQLException {
        boolean result = false;
        Map<AggregationSelectItem, AggregationUnit> aggregationUnitMap = Maps.toMap(selectStatement.getAggregationSelectItems(), new Function<AggregationSelectItem, AggregationUnit>() {
            
            @Override
            public AggregationUnit apply(final AggregationSelectItem input) {
                return AggregationUnitFactory.create(input.getType());
            }
        });
        // 循环顺序合并分组项对应的值相同的记录，比如name=a
        while (currentGroupByValues.equals(new GroupByValue(getCurrentQueryResult(), selectStatement.getGroupByItems()).getGroupValues())) {
            // 条件相同项（聚合项）相加
            aggregate(aggregationUnitMap);
            cacheCurrentRow();
            // 例如，队首结果集前两条name相同，则游标下移后结果集仍在队首，下次循环比较结果相同
            // 若游标下移比较后结果不同，说明没有分组条件相同的记录，终止循环
            result = super.next();
            if (!result) {
                break;
            }
        }
        setAggregationValueToCurrentRow(aggregationUnitMap);
        return result;
    }
    
    private void aggregate(final Map<AggregationSelectItem, AggregationUnit> aggregationUnitMap) throws SQLException {
        for (Entry<AggregationSelectItem, AggregationUnit> entry : aggregationUnitMap.entrySet()) {
            List<Comparable<?>> values = new ArrayList<>(2);
            // 获取每个结果集中聚合项的值
            if (entry.getKey().getDerivedAggregationSelectItems().isEmpty()) {
                values.add(getAggregationValue(entry.getKey()));
            } else {
                for (AggregationSelectItem each : entry.getKey().getDerivedAggregationSelectItems()) {
                    values.add(getAggregationValue(each));
                }
            }
            // 对应值相加
            entry.getValue().merge(values);
        }
    }
    
    private void cacheCurrentRow() throws SQLException {
        for (int i = 0; i < getCurrentQueryResult().getColumnCount(); i++) {
            currentRow.add(getCurrentQueryResult().getValue(i + 1, Object.class));
        }
    }
    
    private Comparable<?> getAggregationValue(final AggregationSelectItem aggregationSelectItem) throws SQLException {
        Object result = getCurrentQueryResult().getValue(aggregationSelectItem.getIndex(), Object.class);
        Preconditions.checkState(null == result || result instanceof Comparable, "Aggregation value must implements Comparable");
        return (Comparable<?>) result;
    }
    
    private void setAggregationValueToCurrentRow(final Map<AggregationSelectItem, AggregationUnit> aggregationUnitMap) {
        for (Entry<AggregationSelectItem, AggregationUnit> entry : aggregationUnitMap.entrySet()) {
            currentRow.set(entry.getKey().getIndex() - 1, entry.getValue().getResult());
        }
    }
    
    @Override
    public Object getValue(final int columnIndex, final Class<?> type) {
        return currentRow.get(columnIndex - 1);
    }
    
    @Override
    public Object getValue(final String columnLabel, final Class<?> type) {
        Preconditions.checkState(labelAndIndexMap.containsKey(columnLabel), "Can't find columnLabel: %s", columnLabel);
        return currentRow.get(labelAndIndexMap.get(columnLabel) - 1);
    }
    
    @Override
    public Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) {
        return currentRow.get(columnIndex - 1);
    }
    
    @Override
    public Object getCalendarValue(final String columnLabel, final Class<?> type, final Calendar calendar) {
        Preconditions.checkState(labelAndIndexMap.containsKey(columnLabel), "Can't find columnLabel: %s", columnLabel);
        return currentRow.get(labelAndIndexMap.get(columnLabel) - 1);
    }
}
