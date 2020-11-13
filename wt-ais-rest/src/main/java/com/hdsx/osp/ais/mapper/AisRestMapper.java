package com.hdsx.osp.ais.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface AisRestMapper {
    /**
     * 查询船舶基础信息
     *
     * @param wktstr
     * @param time
     * @return
     */
    List<Map> getAisInfo(String wktstr, String time);

    /**
     * 根据mmsi查询船舶
     *
     * @param mmsi
     * @return
     */
    Map getAisByMMSI(String mmsi);

    /**
     * 查询船舶航迹信息
     *
     * @param datatime
     * @param time
     * @return
     */
    List<Map> getAisTrail(@Param("datatime") String datatime, @Param("time") String time);

    /**
     * 查询船舶每个时段的位置
     *
     * @param tablename
     * @param time
     * @return
     */
    List<Map> getAisTrailPoint(@Param("tablename") String tablename, @Param("time") String time);

    /**
     * 插入热力图数据表
     * @param datetime
     * @param geomstr
     * @param geomwithriskstr
     */
    void insertData(@Param("datetime") String datetime,@Param("geomstr") String geomstr,@Param("geomwithriskstr") String geomwithriskstr);

    /**
     * 查询船舶风险值数据
     *
     * @param datatime
     * @param gridlevel
     * @return
     */
    List<Map> getAisRiskData(@Param("datatime") String datatime, @Param("time") String time, @Param("gridlevel") String gridlevel);

    /**
     * 清空临时表
     */
    void truncateAisBaseTemp();

    /**
     * 更新操作
     */
    void updateAISBaseFromTempTable();

    /**
     * 清空航迹临时表
     */
    void truncateAisTrailTemp();

    /**
     * 更新航迹操作
     */
    void updateAISTrailFromTempTable();

    /**
     * 清空关联信息临时表
     */
    void truncateAisRelationTemp();

    /**
     * 更新关联信息表
     */
    void updateAISRelationFromTempTable();

    /**
     * 创建航迹分区表
     */
    void createTrailPartitionTable(@Param("tablename") String tablename, @Param("begin") String begin, @Param("end") String end);

    /**
     * 获取所有网格数据
     *
     * @param tablename
     * @return
     */
    List<Map> getGridData(@Param("tablename") String tablename);

    /**
     * 空间查询每个网格包含的船舶信息数据
     *
     * @param tablename
     * @return
     */
    List<Map> getAisGridRelationData(@Param("tablename") String tablename);

    /**
     * 查询是否存在数据表
     *
     * @param tableName
     * @return
     */
    int existTable(@Param("tablename") String tableName);


    /**
     * 查询实时热力数据
     *
     * @param datetime
     * @return
     */
    List<Map> getPresentHeatMapData(@Param("datetime") String datetime);

    /**
     * 查询历史热力数据
     *
     * @param datetime
     * @param timestr
     * @return
     */
    List<Map> getHistoryHeayMapData(@Param("datetime") String datetime, @Param("timestr") String timestr);
}
