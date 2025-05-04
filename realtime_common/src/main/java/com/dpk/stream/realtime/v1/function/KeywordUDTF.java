package com.dpk.stream.realtime.v1.function;

import com.dpk.stream.realtime.v1.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.lzy.stream.realtime.v1.function.KeywordUDTF
 * @Author pengkun_du
 * @Date 2025/4/18 18:57
 * @description: KeywordUDTF
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}