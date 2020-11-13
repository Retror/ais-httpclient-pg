package com.hdsx.osp.ais.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hdsx.osp.ais.mapper.AisRestMapper;
import com.hdsx.osp.ais.util.GeometryUtils;
import com.hdsx.osp.ais.util.JSONUtil;
import com.hdsx.osp.ais.util.ResultJson;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ais查询接口
 */
@RestController
@RequestMapping("/aissearch")
@Slf4j
public class AisRestController {
    @Autowired
    AisRestMapper aisRestMapper;

    /**
     * 获取当前可视域范围内船舶信息
     *
     * @param wktstr 当前可视区域
     * @param time   时间段
     * @return
     */
    @GetMapping("/getAisInfo")
    public Object getAisInfo(@Param("wktstr") String wktstr, @Param("time") String time) {
        try {
            //判断传递的时间段是否是当前时间段，如果是就查询base表，不是则查询trail表
            if (wktstr == null || time == null) {
                log.info("获取当前可视域范围内船舶信息失败，原因{}", "参数为null");
                return ResultJson.success(new ArrayList<>());
            } else {
                LocalDate now = LocalDate.now();
                if (now.toString().equals(time.substring(0, time.indexOf(" ")))) {
                    //查询base表
                    List<Map> list = aisRestMapper.getAisInfo(wktstr, time);
                    log.info("获取当前可视域范围内船舶信息成功...");
                    return ResultJson.success(list);
                } else {
                    //查询航迹表，历史数据[历史热力和历史位置]
                    String datatime = time.substring(0, time.indexOf(" "));
                    List<Map> data = aisRestMapper.getAisTrail(datatime, time);
                    //根据可视域进行空间分析
                    WKTReader reader = new WKTReader();
                    Geometry extent = reader.read(wktstr);
                    List<Integer> integerList = GeometryUtils.getIndexList(extent, GeometryUtils.makePoints(data));
                    List<Map> list = new ArrayList<>();
                    integerList.stream().forEach(l -> {
                        list.add(data.get(l - 1));
                    });
                    List<JSONObject> result = list.stream().map(l -> {
                        JSONObject object = JSON.parseObject(l.get("jsonval").toString());
                        return object;
                    }).collect(Collectors.toList());
                    log.info("获取当前可视域范围内船舶信息成功...");
                    return ResultJson.success(result);
                }
            }
        } catch (Exception e) {
            log.info("获取当前可视域范围内船舶信息失败，原因{}", e.getMessage());
            return ResultJson.success(e.getMessage());
        }
    }


    /**
     * 根据mmsi查询船舶
     *
     * @param mmsi
     * @return
     */
    @GetMapping("/getAisByMMSI")
    public Object getAisByMMSI(@Param("mmsi") String mmsi) {
        try {
            Map map = aisRestMapper.getAisByMMSI(mmsi);
            log.info("根据mmsi查询船舶成功...");
            return ResultJson.success(map);
        } catch (Exception e) {
            log.info("根据mmsi查询船舶失败，原因:{}", e.getMessage());
            return ResultJson.success(e.getMessage());
        }
    }

    /**
     * 查询船舶密度风险值数据
     *
     * @param time      {yyyy-mm-dd hh:mm:ss}
     * @param gridlevel
     * @return
     */
    @GetMapping("/getAISRiskRelation")
    public Object getAISRiskRelation(@Param("time") String time, @Param("gridlevel") String gridlevel) {
        try {
            String datatime = time.substring(0, time.indexOf(" "));
            List<Map> list = aisRestMapper.getAisRiskData(datatime, time, gridlevel);
            log.info("获取船舶风险值数据成功...");
            List<JSONObject> data = list.stream().map(l -> {
                JSONObject object = JSON.parseObject(l.get("jsonval").toString());
                //解码海事局
                object.put("prefecture", JSONUtil.decodeStr(object.get("prefecture").toString()));
                return object;
            }).collect(Collectors.toList());
            return ResultJson.success(data);
        } catch (Exception e) {
            log.info("获取船舶风险值数据失败，原因:{}", e.getMessage());
            return ResultJson.success(e.getMessage());
        }
    }

    /**
     * 获取船舶热力数据
     *
     * @param time
     * @param date
     * @return
     */
    @GetMapping("/getAisHeatMapData")
    public Object getAisHeatMapData(@Param("time") String time, @Param("date") String date) {
        try {
            List<Map> result = new ArrayList<>();
            //判断日期
            //如果是当前时段，则查询实时数据，否则查询历史数据
            String nowDate = LocalDate.now().toString();
            String nowTime = LocalDateTime.now().getHour() + "";
            if (nowDate.equals(date) && nowTime.equals(time)) {
                log.info("查询当前热力数据....");
            } else {
                log.info("查询历史热力数据...");
                result = aisRestMapper.getHistoryHeayMapData(date, date + " " + time);
                log.info("查询船舶热力信息成功...");
            }
            return ResultJson.success(result);
        } catch (Exception e) {
            log.info("查询船舶热力信息失败，原因：{}", e.getMessage());
            return ResultJson.success(e.getMessage());
        }
    }

    /**
     * 插入热力图数据
     *
     * @param time
     * @return
     */
    @GetMapping("/insertHeatMapData")
    public Object insertHeatMapData(@Param("time") String time) {
        try {
            GeometryFactory geometryFactory = new GeometryFactory();
            String datatime = time.substring(0, time.indexOf(" "));
            String tablename = "ais_trail_" + datatime.replace("-", "_");
            log.info("tablename:{},time:{}", tablename, time);
            List<Map> data = aisRestMapper.getAisTrailPoint(tablename, time);
            log.info("长度：{}", data.size());
            //根据时段生成热力数据
            List<Point> points = new ArrayList<>();
            List<Point> riskPoints = new ArrayList<>();
            data.stream().forEach(d -> {
                double lonVal = Double.parseDouble(d.get("lon").toString());
                double latVal = Double.parseDouble(d.get("lat").toString());
                double lenVal = d.get("len") == null ? 0 : Double.parseDouble(d.get("len").toString());
                if (lenVal > 100) {
                    //单独添加船长大于100的热力数据
                    Coordinate riskCoordinate = new Coordinate(lonVal, latVal);
                    riskPoints.add(geometryFactory.createPoint(riskCoordinate));
                }
                Coordinate coordinate = new Coordinate(lonVal, latVal);
                points.add(geometryFactory.createPoint(coordinate));
            });
            MultiPoint multiPoint = geometryFactory.createMultiPoint(points.toArray(new Point[points.size()]));
            String geomStr = GeometryUtils.toGeometryString(multiPoint);
            MultiPoint riskMultiPoint = geometryFactory.createMultiPoint(riskPoints.toArray(new Point[riskPoints.size()]));
            String riskGeomStr = GeometryUtils.toGeometryString(riskMultiPoint);
            //插入表
            aisRestMapper.insertData(time, geomStr, riskGeomStr);
            log.info("数据插入成功...");
            return ResultJson.success("数据插入成功...");
        } catch (Exception e) {
            log.info("数据插入失败，原因：{}", e.getMessage());
            return ResultJson.success(e.getMessage());
        }
    }
}
