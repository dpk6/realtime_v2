package com.dpk.stream.realtime.v1.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.dpk.retail.v1.realtime.bean.DimJoinFunction
 * @Author pengkun.du
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
