package com.hdsx.osp.ais.bean;

import java.util.Objects;

public class JsonData {
    public String date;
    public String gid;
    public String area;
    public String gridtype;
    public String prefecture;
    public Double count;

    public JsonData(String date, String gid, String area, String gridtype, String prefecture) {
        this.date = date;
        this.gid = gid;
        this.area = area;
        this.gridtype = gridtype;
        this.prefecture = prefecture;
    }

    public JsonData(String date, String gid, String area, String gridtype, String prefecture, Double count) {
        this.date = date;
        this.gid = gid;
        this.area = area;
        this.gridtype = gridtype;
        this.prefecture = prefecture;
        this.count = count;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getGridtype() {
        return gridtype;
    }

    public void setGridtype(String gridtype) {
        this.gridtype = gridtype;
    }

    public String getPrefecture() {
        return prefecture;
    }

    public void setPrefecture(String prefecture) {
        this.prefecture = prefecture;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonData jsonData = (JsonData) o;
        return Objects.equals(date, jsonData.date)
                && Objects.equals(gid, jsonData.gid)
                && Objects.equals(area, jsonData.area)
                && Objects.equals(gridtype, jsonData.gridtype)
                && Objects.equals(prefecture, jsonData.prefecture)
                && Objects.equals(count, jsonData.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, gid, area, gridtype, prefecture);
    }

    @Override
    public String toString() {
        return "JsonData{" +
                "date='" + date + '\'' +
                ", gid='" + gid + '\'' +
                ", area='" + area + '\'' +
                ", gridtype='" + gridtype + '\'' +
                ", prefecture='" + prefecture + '\'' +
                ", count=" + count +
                '}';
    }
}
