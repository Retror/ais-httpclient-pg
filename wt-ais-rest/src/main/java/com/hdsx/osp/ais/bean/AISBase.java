package com.hdsx.osp.ais.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hdsx.osp.ais.util.GeometryUtils;
import com.vividsolutions.jts.geom.Point;
import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Data
public class AISBase {
    private String id;
    //消息类型
    private String type;
    //转发指示符
    private String forward;
    //船舶mmsi
    private String mmsi;
    //导航状态
    private String navistat;
    //转向率
    private String rot;
    //航速，单位1/10节
    private String sog;
    //位置准确度
    private String posacur;
    //经度
    private String longitude;
    //纬度
    private String latitude;
    //对地航向，1/10度为单位
    private String cog;
    //真艏向
    private String thead;
    //ais时间戳
    private String utctime;
    //特定操作指示符
    private String indicator;
    //raim标志
    private String raim;
    //接收时间戳
    private String receivetime;
    //B类装置标志
    private String devicemark;
    //B类显示器标志
    private String dispmark;
    //B类DSC标志
    private String dscmark;
    //B类带宽标志
    private String bandmark;
    //B类消息22标志
    private String msg22mark;
    //B类指配模式标志
    private String patternmark;
    //当前GNSS位置状态
    private String gnss;
    //AIS版本指示符
    private String ver;
    //IMO编号
    private String imo;
    //呼号
    private String callno;
    //名称
    private String shipname;
    //船舶和货物类型
    private String shipAndCargType;
    //船长
    private String length;
    //船宽
    private String width;
    //电子定位装置的类型
    private String devicetype;
    //预计到港时间
    private String eta;
    //目前最大静态吃水
    private String draft;
    //目的港
    private String dest;
    //DTE
    private String dte;

    private String timeToString() {
        DateTimeFormatter fomatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        Instant instant = Instant.ofEpochMilli(Long.parseLong(this.receivetime));
        LocalDateTime ld = instant.atOffset(ZoneOffset.ofHours(8)).toLocalDateTime();
        return ld.toString();
    }

    private void appendSplit(StringBuffer stringBuffer) {
        stringBuffer.append("\t");
    }

    private void appendString(String value, StringBuffer stringBuffer) {
        this.appendString(value, stringBuffer, true);
    }

    private void appendJSONString(AISBase aisBase, StringBuffer stringBuffer, String dateTimeStr) {
        this.appendJSONString(aisBase, stringBuffer, true, dateTimeStr);
    }

    private void appendJSONString(AISBase aisBase, StringBuffer stringBuffer, boolean isAppendSplit, String dateTimeStr) {
        if (aisBase == null) {
            stringBuffer.append("\\N");
        } else {
            JSONObject obj = (JSONObject) JSON.toJSON(aisBase);
            obj.put("datetimestr",dateTimeStr);
            JSONArray array = new JSONArray();
            array.add(obj);
            String jsonStr = JSON.toJSONString(array);
            jsonStr = jsonStr.replace("\"", "\"\"");
            jsonStr = "\"" + jsonStr + "\"";
            stringBuffer.append(jsonStr);
        }
        if (isAppendSplit) {
            this.appendSplit(stringBuffer);
        }
    }

    public String toAISBaseStr() {
        StringBuffer buffer = new StringBuffer();
        this.appendString("mmsi", buffer);
        this.appendString("type", buffer);
        this.appendString("forward", buffer);
        this.appendString("navistat", buffer);
        this.appendString("rot", buffer);
        this.appendString("sog", buffer);
        this.appendString("posacur", buffer);
        this.appendString("longitude", buffer);
        this.appendString("latitude", buffer);
        this.appendString("cog", buffer);
        this.appendString("thead", buffer);
        this.appendString("utctime", buffer);
        this.appendString("indicator", buffer);
        this.appendString("raim", buffer);
        this.appendString("devicemark", buffer);
        this.appendString("dispmark", buffer);
        this.appendString("dscmark", buffer);
        this.appendString("bandmark", buffer);
        this.appendString("msg22mark", buffer);
        this.appendString("patternmark", buffer);
        this.appendString("gnss", buffer);
        this.appendString("ver", buffer);
        this.appendString("imo", buffer);
        this.appendString("callno", buffer);
        this.appendString("shipname", buffer);
        this.appendString("shipAndCargType", buffer);
        this.appendString("length", buffer);
        this.appendString("width", buffer);
        this.appendString("devicetype", buffer);
        this.appendString("eta", buffer);
        this.appendString("draft", buffer);
        this.appendString("dest", buffer);
        this.appendString("dte", buffer);
        this.appendString("geom", buffer);
        this.appendString("receivetime", buffer);
        this.appendString("updatetime", buffer);
        return buffer.toString();
    }

    public String toAISBaseSQLString(String dateStr) {
        StringBuffer buffer = new StringBuffer();
        this.appendString(this.mmsi, buffer);
        this.appendString(this.type, buffer);
        this.appendString(this.forward, buffer);
        this.appendString(this.navistat, buffer);
        this.appendString(this.rot, buffer);
        this.appendString(this.sog, buffer);
        this.appendString(this.posacur, buffer);
        this.appendString(this.longitude, buffer);
        this.appendString(this.latitude, buffer);
        this.appendString(this.cog, buffer);
        this.appendString(this.thead, buffer);
        this.appendString(this.utctime, buffer);
        this.appendString(this.indicator, buffer);
        this.appendString(this.raim, buffer);
        this.appendString(this.devicemark, buffer);
        this.appendString(this.dispmark, buffer);
        this.appendString(this.dscmark, buffer);
        this.appendString(this.bandmark, buffer);
        this.appendString(this.msg22mark, buffer);
        this.appendString(this.patternmark, buffer);
        this.appendString(this.gnss, buffer);
        this.appendString(this.ver, buffer);
        this.appendString(this.imo, buffer);
        this.appendString(this.callno, buffer);
        this.appendString(this.shipname, buffer);
        this.appendString(this.shipAndCargType, buffer);
        this.appendString(this.length, buffer);
        this.appendString(this.width, buffer);
        this.appendString(this.devicetype, buffer);
        this.appendString(this.eta, buffer);
        this.appendString(this.draft, buffer);
        this.appendString(this.dest, buffer);
        this.appendString(this.dte, buffer);
        //添加空间字段
        this.appendGeom(buffer);
        //日期单独处理进行格式化
        buffer.append(timeToString());
        this.appendSplit(buffer);
        this.appendString(dateStr,buffer);
        this.appendString(this.mmsi, buffer, false);//geoserver 中无法显示主键，新建了一个主键为ID，实际存放的是mmsi

        return buffer.toString();
    }

    public String toAISTrailStr() {
        StringBuffer buffer = new StringBuffer();
        this.appendString("mmsi", buffer);
        this.appendString("jsondata", buffer);
        this.appendString("geom", buffer);
        this.appendString("rfield2", buffer);
        this.appendString("rfield3", buffer);
        this.appendString("rfield4", buffer);
        this.appendString("datatime", buffer);
        return buffer.toString();
    }

    public String toAISTrailSQLString(String dateTimeStr) {
        StringBuffer buffer = new StringBuffer();
        this.appendString(this.mmsi, buffer);
        this.appendJSONString(this, buffer, dateTimeStr);
        //添加空间字段
        //this.appendGeom(buffer);
        this.appendString("", buffer);//geom
        this.appendString("", buffer);//rfield2
        this.appendString("", buffer);//rfield3
        this.appendString("", buffer);//rfield4
        //日期单独处理进行格式化
        buffer.append(timeToString());
        this.appendSplit(buffer);
        this.appendString(this.mmsi, buffer, false);//geoserver 中无法显示主键，新建了一个主键为ID，实际存放的是mmsi
        return buffer.toString();
    }

    private void appendGeom(StringBuffer stringBuffer) {
        appendGeom(stringBuffer, true);
    }

    private void appendGeom(StringBuffer stringBuffer, boolean isAppendSplit) {
        if (this.isNotNull(this.longitude) && this.isNotNull(this.latitude)) {
            stringBuffer.append(geometryToString());
        } else {
            stringBuffer.append("\\N");
        }
        if (isAppendSplit) {
            this.appendSplit(stringBuffer);
        }
    }

    private String geometryToString() {
        Point point = GeometryUtils.createPoint(Double.parseDouble(this.longitude), Double.parseDouble(this.latitude));
        point.setSRID(4326);
        return GeometryUtils.toGeometryString(point);
    }

    private void appendString(String value, StringBuffer stringBuffer, boolean isAppendSplit) {
        if (value == null) {
            stringBuffer.append("\\N");
        } else {
            stringBuffer.append(escapeSql(value.replace("\"","")));
        }
        if (isAppendSplit) {
            this.appendSplit(stringBuffer);
        }
    }

    private String escapeSql(String value) {
        return value.replaceAll("\\\\", "");
    }

    private boolean isNotNull(String str) {
        boolean flag = true;
        if (str == null) {
            flag = false;
        } else {
            if (str.trim().length() == 0) {
                flag = false;
            } else {
                if (!str.equals("0")) {
                    flag = true;
                }
            }
        }
        return flag;
    }

    /**
     * 合并对象
     *
     * @param target
     * @return
     */
    public AISBase merge(AISBase target) {
        if (target == null) {
            return this;
        }
        if (target.type != null) {
            this.type = target.type;
        }
        if (target.forward != null) {
            this.forward = target.forward;
        }
        if (target.mmsi != null) {
            this.mmsi = target.mmsi;
        }
        if (target.navistat != null) {
            this.navistat = target.navistat;
        }
        if (target.rot != null) {
            this.rot = target.rot;
        }
        if (target.sog != null) {
            this.sog = target.sog;
        }
        if (target.posacur != null) {
            this.posacur = target.posacur;
        }
        if (target.longitude != null) {
            this.longitude = target.longitude;
        }
        if (target.latitude != null) {
            this.latitude = target.latitude;
        }
        if (target.cog != null) {
            this.cog = target.cog;
        }
        if (target.thead != null) {
            this.thead = target.thead;
        }
        if (target.utctime != null) {
            this.utctime = target.utctime;
        }
        if (target.indicator != null) {
            this.indicator = target.indicator;
        }
        if (target.raim != null) {
            this.raim = target.raim;
        }
        if (target.receivetime != null) {
            this.receivetime = target.receivetime;
        }
        if (target.devicemark != null) {
            this.devicemark = target.devicemark;
        }
        if (target.dispmark != null) {
            this.dispmark = target.dispmark;
        }
        if (target.dscmark != null) {
            this.dscmark = target.dscmark;
        }
        if (target.bandmark != null) {
            this.bandmark = target.bandmark;
        }
        if (target.msg22mark != null) {
            this.msg22mark = target.msg22mark;
        }
        if (target.patternmark != null) {
            this.patternmark = target.patternmark;
        }
        if (target.gnss != null) {
            this.gnss = target.gnss;
        }

        if (target.ver != null) {
            this.ver = target.ver;
        }
        if (target.imo != null) {
            this.imo = target.imo;
        }
        if (target.callno != null) {
            this.callno = target.callno;
        }
        if (target.shipname != null) {
            this.shipname = target.shipname;
        }
        if (target.shipAndCargType != null) {
            this.shipAndCargType = target.shipAndCargType;
        }
        if (target.length != null) {
            this.length = target.length;
        }
        if (target.width != null) {
            this.width = target.width;
        }
        if (target.devicetype != null) {
            this.devicetype = target.devicetype;
        }
        if (target.eta != null) {
            this.eta = target.eta;
        }
        if (target.draft != null) {
            this.draft = target.draft;
        }
        if (target.dest != null) {
            this.dest = target.dest;
        }
        if (target.dte != null) {
            this.dte = target.dte;
        }
        return null;
    }
}
