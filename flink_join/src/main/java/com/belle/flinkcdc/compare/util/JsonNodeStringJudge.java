package com.belle.flinkcdc.compare.util;

import org.apache.flink.calcite.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author : zhuhaohao
 * @date :
 */
public class JsonNodeStringJudge {
    public static void main(String[] args) throws JsonProcessingException {
        String strJson = "{\n" +
                "  \"header\": {\n" +
                "    \"catalog\": \"order_db\",\n" +
                "    \"table\": \"tbl_order_sale_apply_refund_main\",\n" +
                "    \"readTime\": 1667750401000,\n" +
                "    \"recordType\": \"realtime\",\n" +
                "    \"sourceType\": \"Mysql\",\n" +
                "    \"pkValue\": \"1d9c5ffb5c36429bb406388185dde641\"\n" +
                "  },\n" +
                "  \"operation\": \"insert\",\n" +
                "  \"location\": {\n" +
                "    \"id\": null,\n" +
                "    \"refund_main_no\": \"188800022388054630\",\n" +
                "    \"total_back_amount\": 350.18,\n" +
                "    \"taobao_state\": \"WAIT_BUYER_RETURN_GOODS\",\n" +
                "    \"taobao_refund_type\": 4,\n" +
                "    \"product_no\": \"101295805003\",\n" +
                "    \"refund_reason\": \"7天无理由退换货\",\n" +
                "    \"sub_detail_no\": \"2997062607675053046\",\n" +
                "    \"order_source_no\": \"TB-TBZXD-STACCATO\",\n" +
                "    \"out_order_id\": \"2997062607673053046\",\n" +
                "    \"creator\": \"system\",\n" +
                "    \"update_name\": null,\n" +
                "    \"update_time\": \"2022-11-07 00:00:01\",\n" +
                "    \"create_time\": \"2022-11-07 00:00:01\",\n" +
                "    \"remark\": \"\",\n" +
                "    \"refund_oid\": \"[{\\\"detRefundnum\\\":1,\\\"detLevelCode\\\":\\\"\\\",\\\"detailNo\\\":\\\"2997062607675053046\\\",\\\"detRefundAmt\\\":0,\\\"detRefRealitynum\\\":1}]\",\n" +
                "    \"auto_refund_time\": \"2022-11-13 23:59:54\",\n" +
                "    \"advance_status\": \"0\",\n" +
                "    \"refund_phase\": \"onsale\",\n" +
                "    \"expand_price_used\": 0,\n" +
                "    \"plat_ref_handle\": null,\n" +
                "    \"refund_status\": \"1\"\n" +
                "  },\n" +
                "  \"data\": {\n" +
                "    \"meta\": [\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"id\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"refund_main_no\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 22,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 8,\n" +
                "        \"columnTypeName\": \"double\",\n" +
                "        \"columnName\": \"total_back_amount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"taobao_state\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 10,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 4,\n" +
                "        \"columnTypeName\": \"int\",\n" +
                "        \"columnName\": \"taobao_refund_type\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"product_no\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 256,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"refund_reason\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"sub_detail_no\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"order_source_no\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"out_order_id\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"creator\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 32,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"update_name\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 19,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 93,\n" +
                "        \"columnTypeName\": \"datetime\",\n" +
                "        \"columnName\": \"update_time\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 19,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 93,\n" +
                "        \"columnTypeName\": \"datetime\",\n" +
                "        \"columnName\": \"create_time\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 256,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"remark\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 4000,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"refund_oid\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 19,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 93,\n" +
                "        \"columnTypeName\": \"datetime\",\n" +
                "        \"columnName\": \"auto_refund_time\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 10,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"advance_status\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 20,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"refund_phase\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 10,\n" +
                "        \"scale\": 2,\n" +
                "        \"columnType\": 3,\n" +
                "        \"columnTypeName\": \"decimal\",\n" +
                "        \"columnName\": \"expand_price_used\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 6,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"plat_ref_handle\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"precision\": 8,\n" +
                "        \"scale\": 0,\n" +
                "        \"columnType\": 12,\n" +
                "        \"columnTypeName\": \"varchar\",\n" +
                "        \"columnName\": \"refund_status\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": [\n" +
                "      \"1d9c5ffb5c36429bb406388185dde641\",\n" +
                "      \"188800022388054630\",\n" +
                "      350.18,\n" +
                "      \"WAIT_BUYER_RETURN_GOODS\",\n" +
                "      4,\n" +
                "      \"101295805003\",\n" +
                "      \"7天无理由退换货\",\n" +
                "      \"2997062607675053046\",\n" +
                "      \"TB-TBZXD-STACCATO\",\n" +
                "      \"2997062607673053046\",\n" +
                "      \"system\",\n" +
                "      null,\n" +
                "      \"2022-11-07 00:00:01\",\n" +
                "      \"2022-11-07 00:00:01\",\n" +
                "      \"\",\n" +
                "      \"[{\\\"detRefundnum\\\":1,\\\"detLevelCode\\\":\\\"\\\",\\\"detailNo\\\":\\\"2997062607675053046\\\",\\\"detRefundAmt\\\":0,\\\"detRefRealitynum\\\":1}]\",\n" +
                "      \"2022-11-13 23:59:54\",\n" +
                "      \"0\",\n" +
                "      \"onsale\",\n" +
                "      0,\n" +
                "      null,\n" +
                "      \"1\"\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonNode = mapper.readTree(strJson);


        JsonNode location = jsonNode.get("location");

        JsonNode id = location.get("id");
       if (id != null){
           System.out.println(id.toString());

       }else {
           System.out.println("没有当前字符串");

       }


        StringBuilder result = new StringBuilder();
       result.append("nihao");
       result.append("buhao");
       result.append(12);
        System.out.println(result);

    }
}
