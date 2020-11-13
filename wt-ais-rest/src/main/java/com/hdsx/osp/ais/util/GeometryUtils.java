package com.hdsx.osp.ais.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hdsx.osp.ais.bean.Points;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class GeometryUtils {
    private static GeometryFactory geometryFactory = new GeometryFactory();
    private static WKTReader wktReader = new WKTReader();

    public static double meterToRadian(double meters) {
        double radian = meters * 8.983152841195214E-6D;
        return radian;
    }

    public static double radianToMeter(double radian) {
        double meters = radian / 8.983152841195214E-6D;
        return meters;
    }

    public static Point createPoint(double x, double y) {
        Coordinate coord = new Coordinate(x, y);
        Point point = geometryFactory.createPoint(coord);
        return point;
    }

    public static String toGeometryString(Geometry geometry) {
        WKBWriter wkbWriter = new WKBWriter(2, true);
        String hexString = WKBWriter.toHex(wkbWriter.write(geometry));
        return hexString;
    }

    /**
     * 构造Geometry
     *
     * @param wkt
     * @return
     */
    public static Geometry wktToGeometry(String wkt) {
        Geometry geometry = null;
        try {
            geometry = wktReader.read(wkt);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return geometry;
    }

    /**
     * 创建点
     *
     * @param data
     * @return
     */
    public static List<Points> makePoints(List<Map> data) {
        List<Points> points = new ArrayList<>();
        AtomicInteger atomicInteger = new AtomicInteger();
        data.stream().forEach(d -> {
            atomicInteger.getAndIncrement();
            JSONObject object = JSON.parseObject(d.get("jsonval").toString());
            double longitude = Double.parseDouble(object.get("longitude").toString());
            double latitude = Double.parseDouble(object.get("latitude").toString());
            Points p = new Points(longitude, latitude, atomicInteger.get());
            points.add(p);
        });
        return points;
    }

    //获取在视野范围内的点的下标
    public static List<Integer> getIndexList(Geometry extent, List<Points> points) {
        List<Integer> list = new ArrayList<>();
        points.stream().forEach(p -> {
            String wktStr = "POINT (" + p.getX() + " " + p.getY() + ")";
            Geometry pt = wktToGeometry(wktStr);
            if (extent.contains(pt)) {
                list.add(p.getIndex());
            }
        });
        return list;
    }
}
