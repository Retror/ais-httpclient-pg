package com.hdsx.osp.ais.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import static org.apache.commons.lang.StringEscapeUtils.escapeSql;

/**
 * 网格基础映射类
 */
@Data
public class GridRelation {
    //日期
    private String datatime;
    //存储船舶密度信息和网格对应关系
    private JSONArray jsondata;
    //网格类型
    private String gridlevel;

    public String toAISRelationStr() {
        StringBuffer buffer = new StringBuffer();
        this.appendString("datatime", buffer);
        this.appendString("jsondata", buffer);
        this.appendString("gridlevel", buffer);
        return buffer.toString();
    }

    public String toAISRelationSQLString() {
        StringBuffer buffer = new StringBuffer();
        this.appendString(this.datatime, buffer);
        this.appendJSONString(this.jsondata, buffer);
        this.appendString(this.gridlevel, buffer,false);
        return buffer.toString();
    }

    private void appendSplit(StringBuffer stringBuffer) {
        stringBuffer.append("\t");
    }

    private void appendString(String value, StringBuffer stringBuffer) {
        this.appendString(value, stringBuffer, true);
    }

    private void appendJSONString(JSONArray array, StringBuffer stringBuffer) {
        this.appendJSONString(array, stringBuffer, true);
    }

    private void appendString(String value, StringBuffer stringBuffer, boolean isAppendSplit) {
        if (value == null) {
            stringBuffer.append("\\N");
        } else {
            stringBuffer.append(escapeSql(value));
        }
        if (isAppendSplit) {
            this.appendSplit(stringBuffer);
        }
    }

    private void appendJSONString(JSONArray array, StringBuffer stringBuffer, boolean isAppendSplit) {
        if (array == null) {
            stringBuffer.append("\\N");
        } else {
            String jsonStr = JSON.toJSONString(array);
            jsonStr = jsonStr.replace("\"", "\"\"");
            jsonStr = "\"" + jsonStr + "\"";
            stringBuffer.append(jsonStr);
        }
        if (isAppendSplit) {
            this.appendSplit(stringBuffer);
        }
    }
}
