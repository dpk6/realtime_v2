package com.dpk.stream.realtime.v1.app.ods;

import com.dpk.stream.realtime.v1.utils.FlinkSinkUtil;
import com.dpk.stream.realtime.v1.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCkafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //读取mysql数据库所有表数据
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");
        //创建流式处理
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mySQLSource.print();
        //创建kafka主题写入数据
        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

        mySQLSource.sinkTo(topic_db);


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
