package com.dpk.stream.realtime.v1.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dpk.stream.realtime.v1.bean.TableProcessDim;
import com.dpk.stream.realtime.v1.constant.Constant;
import com.dpk.stream.realtime.v1.function.HBaseSinkFunction;
import com.dpk.stream.realtime.v1.function.TableProcessFunction;
import com.dpk.stream.realtime.v1.utils.FlinkSourceUtil;
import com.dpk.stream.realtime.v1.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.lzy.stream.realtime.v1.base.BaseApp
 * @Author pengkun_du
 * @Date 2025/4/11 8:39
 * @description: BaseApp
 */

public class BaseApp {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 启用检查点，每5秒做一次，保证exactly-once语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 从Kafka读取业务数据变更
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 处理Kafka数据，过滤出需要的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        // 解析JSON数据
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
                        String data = jsonObj.getString("after");

                        // 只处理realtime_v1数据库的增删改查操作
                        if ("realtime_v1".equals(db)
                                && ("c".equals(type) || "u".equals(type) || "d".equals(type) || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 从MySQL读取维度表配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v2", "table_process_dim");
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

        // 解析维度表配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim;
                        // 根据操作类型获取配置信息
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        // 处理维度表配置变更，创建或删除HBase表
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取HBase连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭HBase连接
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");

                        // 根据操作类型处理HBase表
                        if("d".equals(op)){
                            // 删除表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            // 创建表
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }else{
                            // 更新表：先删除后创建
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

        // 创建广播状态描述器，用于存储维度表配置信息
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);

        // 将维度表配置信息广播出去
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // 将业务数据流和配置信息流连接起来
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 处理连接后的数据流，根据配置信息将数据写入HBase
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

        // 打印处理结果
        dimDS.print();

        // 将数据写入HBase
        dimDS.addSink(new HBaseSinkFunction());

        // 执行作业
        env.execute("dim");
    }
}
