package com.xh.spark.sql.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 京东指标还原
 * 该类用来判断套餐下对应单品或套餐的还原规则
 * 传入的数据格式为array(struct)类型
 * sku：产品型号词，单品之间使用+号分割
 * cate：产品类目（烟灶、嵌入式、大套系、烟灶消）
 * flag：该sku的前二个产品是否是烟灶套餐（仅在cate是大套系、烟灶消情况下使用）
 */
public class JDQuotaReduction extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector args) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> resType = new ArrayList<>();
        fieldNames.add("rela_sku");
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("rule_id");
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, resType);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        List<Object> allQuota = (ArrayList<Object>) record[0];
        List<Object> firstQuotaValueOrder = (ArrayList<Object>) allQuota.get(0);
        String[] sku = firstQuotaValueOrder.get(0).toString().split("\\+");
        String cate = firstQuotaValueOrder.get(1).toString();
        String flag = firstQuotaValueOrder.get(2).toString();
        // 烟灶套餐
        if (cate != null && "烟灶".equals(cate)) {
            Object[] result = new Object[2];
            //第一件单品
            result[0] = sku[0];
            result[1] = "0";
            forward(result);
            //第二件的单品
            result[0] = sku[1];
            result[1] = "1";
            forward(result);
            return;
        }
        // 嵌入式套餐
        if (cate != null && "嵌入式".equals(cate)) {
            Object[] result = new Object[2];
            // 第一件单品
            result[0] = sku[0];
            result[1] = "2";
            forward(result);
            // 第二件及之后的单品
            for (int i = 1; i < sku.length; i++) {
                result[0] = sku[i];
                result[1] = "1";
                forward(result);
            }
            return;
        }
        // 烟灶消套餐及大套系
        if (cate != null && ("烟灶消".equals(cate) || "大套系".equals(cate))) {
            Object[] result = new Object[2];
            // 第二件及之后的单品
            for (int i = 1; i < sku.length; i++) {
                result[0] = sku[i];
                result[1] = "1";
                forward(result);
            }

            if (flag != null && "1".equals(flag)) {
                // 第一件+第二件的组合是烟灶套餐
                result[0] = sku[0] + "+" + sku[1];
                result[1] = 3;
                forward(result);
            } else {
                result[0] = sku[0];
                result[1] = "2";
                forward(result);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
