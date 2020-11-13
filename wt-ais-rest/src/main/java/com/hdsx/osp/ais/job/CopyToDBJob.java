package com.hdsx.osp.ais.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hdsx.osp.ais.bean.AISBase;
import com.hdsx.osp.ais.bean.GridRelation;
import com.hdsx.osp.ais.bean.JsonData;
import com.hdsx.osp.ais.mapper.AisRestMapper;
import com.hdsx.osp.ais.util.DBCopyUtil;
import com.hdsx.osp.ais.util.GeometryUtils;
import com.hdsx.osp.ais.util.JSONUtil;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 1.读取HttpClient接口，将AIS数据写出文件；
 * 2.然后使用fastjson进行解析，读取成csv文件；
 * 3.使用copy指令入库，保存或者更新AisBase表；
 * 4.删除json文件和csv文件
 * 5.更新或者新增aistrail数据表；
 * 6.计算船舶密度，更新或者新增关系；
 */
@Slf4j
public class CopyToDBJob implements Job {
    @Value("${task.filepath}")
    String filepath;
    @Value("${task.httpclienturl}")
    String httpclienturl;
    @Value("${task.apikey}")
    String apikey;
    @Value("${task.lb}")
    String lb;
    @Value("${task.rt}")
    String rt;
    @Autowired
    AisRestMapper aisRestMapper;
    private Map<String, AISBase> aisBaseMap = new ConcurrentHashMap<>();//基础信息
    //所有网格数据表名称
    private List<String> gridList = Arrays.asList(new String[]{"grid_1_2_degree", "grid_1_2_min", "grid_1_4_degree", "grid_1_4_min", "grid_1_degree", "grid_1_min", "grid_10_min"});

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        //存储网格数据
        List<Map> dataList = new ArrayList<>();
        LocalDate now = LocalDate.now();
        String timeStr = now.toString().replace("-", "_");
        String tableName = "ais_trail_" + timeStr;
        //表不存在则创建表
        if (existTable(tableName) < 1) {
            String begin = now.toString();
            String end = now.plusDays(1).toString();
            //创建分区表
            aisRestMapper.createTrailPartitionTable(tableName, begin, end);
        }
        //记录每个小时的时刻
        String dateTimeStr = getTimeStr(LocalDateTime.now());
        long jobBeginTime = System.currentTimeMillis();
        log.info("任务开始执行...");
        //1.读取HTTPClient接口，将获取的数据写出文件
        try {
            //读取httpclient数据，并且写出文件
            getAisData(httpclienturl, apikey, lb, rt);
            String jsonFile = filepath + "aisData.json";
            //使用fastjson读取数据
            List<AISBase> list = JSONUtil.ReadJSONFile(jsonFile);
            //写出csv文件
            list.stream().forEach(l -> {
                //批量更新AIS实时信息，此处不适合使用map去重复，因为会导致更新时间太长
                AISBase base = aisBaseMap.get(l.getMmsi());
                if (base == null) {
                    aisBaseMap.put(l.getMmsi(), l);
                } else {
                    base.merge(l);
                }
            });
            //进行数据库操作(船舶基础表)
            copyToAISBaseDB(aisBaseMap, dateTimeStr);
            //入库航迹信息(更新或者插入)
            copyToAISTrailDB(aisBaseMap, dateTimeStr);
            //入库操作完成后，进行数据计算

            //查询所有网格，计算数据
            gridList.stream().forEach(g -> {
                //List<Map> gridData = aisRestMapper.getGridData(g);
                List<Map> gridData = aisRestMapper.getAisGridRelationData(g);
                List<Map> resultMap = new ArrayList<>();
                gridData.stream().forEach(gd -> {
                    if (gd.get("area") != null) {
                        Map map = new ConcurrentHashMap();
                        //添加网格类型
                        map.put("gridtype", g);
                        //将平方公里转换为平方海里
                        BigDecimal b1 = new BigDecimal(gd.get("area").toString());
                        //1平方海里等于3.43平方公里
                        BigDecimal b2 = new BigDecimal("3.43");
                        double area = b1.divide(b2, 12, BigDecimal.ROUND_HALF_UP).doubleValue();
                        map.put("area", area);
                        map.put("gid", gd.get("gid").toString());
                        map.put("length", gd.get("length").toString());
                        map.put("prefecture", gd.get("prefecture").toString());
                        resultMap.add(map);
                    }
                });
                dataList.addAll(resultMap);
            });
            //size = 31720
            /**
             * 计算当前所有数据和所有7个网格的关联关系
             */
            calcAndOperateRelationWithDB(dataList, dateTimeStr);

            //将每个时段的数据插入至热力图数据表
            insertHeatMapData(dateTimeStr);
        } catch (Exception e) {
            log.info("任务执行错误,原因:{}", e.getMessage());
        }
        long jobEndTime = System.currentTimeMillis();
        log.info("任务执行结束，用时：{}", jobEndTime - jobBeginTime);

    }

    /**
     * 执行copy指令写入数据库数据
     *
     * @param aisBaseMap
     */
    private void copyToAISBaseDB(Map<String, AISBase> aisBaseMap, String dateTimeStr) {
        try {
            String filePath = filepath + "ais_base_temp.csv";
            createCSVFile(aisBaseMap, filePath, dateTimeStr);
            //每次操作前删除上一次的临时数据，防止上次操作异常影响本次
            aisRestMapper.truncateAisBaseTemp();
            //执行copy操作
            String status = DBCopyUtil.restoreJob(filePath, "ais_base_temp");
            //进行数据库数据操作
            aisRestMapper.updateAISBaseFromTempTable();
            //删除csv
            if (status.equals("success")) {
                new File(filePath).delete();
            }
        } catch (Exception e) {
            log.info("数据写入错误,错误原因:{}", e.getMessage());
        }

    }

    /**
     * 生成csv文件
     *
     * @param aisBaseMap
     * @param filePath
     * @return
     */
    public File createCSVFile(Map<String, AISBase> aisBaseMap, String filePath, String dateTimeStr) {
        File file = new File(filePath);
        boolean isExists = true;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("创建文件失败：" + e);
                isExists = false;
            }
        }
        if (!isExists) {
            return null;
        }
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(file));
            //写入文件头
            String headerStr = new AISBase().toAISBaseStr();
            out.write(headerStr);
            out.write("\r\n");
            Iterator iterators = aisBaseMap.keySet().iterator();
            while (iterators.hasNext()) {
                AISBase aisBase = aisBaseMap.get(iterators.next());
                String str = null;
                str = aisBase.toAISBaseSQLString(dateTimeStr);
                out.write(str);
                out.write("\r\n");
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    /**
     * 执行copy指令写入数据库数据
     *
     * @param aisBaseMap
     */
    private void copyToAISTrailDB(Map<String, AISBase> aisBaseMap, String dateTimeStr) {
        try {
            String filePath = filepath + "ais_trail_temp.csv";
            createTrailCSVFile(aisBaseMap, filePath, dateTimeStr);
            //每次操作前删除上一次的临时数据，防止上次操作异常影响本次
            aisRestMapper.truncateAisTrailTemp();
            //执行copy操作
            String status = DBCopyUtil.restoreJob(filePath, "ais_trail_temp");
            //进行数据库数据操作
            aisRestMapper.updateAISTrailFromTempTable();
            //删除csv
            if (status.equals("success")) {
                new File(filePath).delete();
            }
        } catch (Exception e) {
            log.info("数据写入错误,错误原因:{}", e.getMessage());
        }
    }

    /**
     * 生成csv文件
     *
     * @param aisBaseMap
     * @param filePath
     * @return
     */
    public File createTrailCSVFile(Map<String, AISBase> aisBaseMap, String filePath, String dateTimeStr) {
        File file = new File(filePath);
        boolean isExists = true;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("创建文件失败：" + e);
                isExists = false;
            }
        }
        if (!isExists) {
            return null;
        }
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(file));
            //写入文件头
            String headerStr = new AISBase().toAISTrailStr();
            out.write(headerStr);
            out.write("\r\n");
            Iterator iterators = aisBaseMap.keySet().iterator();
            while (iterators.hasNext()) {
                AISBase aisBase = aisBaseMap.get(iterators.next());
                String str = null;
                str = aisBase.toAISTrailSQLString(dateTimeStr);
                out.write(str);
                out.write("\r\n");
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    /**
     * 执行copy指令写入数据库数据
     *
     * @param aisRelationMap
     */
    private void copyToAISRelationDB(Map<String, GridRelation> aisRelationMap) {
        try {
            String filePath = filepath + "ais_grid_relation_temp.csv";
            createRelationCSVFile(aisRelationMap, filePath);
            //每次操作前删除上一次的临时数据，防止上次操作异常影响本次
            aisRestMapper.truncateAisRelationTemp();
            //执行copy操作
            String status = DBCopyUtil.restoreJob(filePath, "ais_grid_relation_temp");
            //进行数据库数据操作
            aisRestMapper.updateAISRelationFromTempTable();
            //删除csv
            if (status.equals("success")) {
                new File(filePath).delete();
            }
        } catch (Exception e) {
            log.info("数据写入错误,错误原因:{}", e.getMessage());
        }
    }

    /**
     * 生成csv文件
     *
     * @param aisRelationMap
     * @param filePath
     * @return
     */
    public File createRelationCSVFile(Map<String, GridRelation> aisRelationMap, String filePath) {
        File file = new File(filePath);
        boolean isExists = true;
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("创建文件失败：" + e);
                isExists = false;
            }
        }
        if (!isExists) {
            return null;
        }
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(file));
            //写入文件头
            String headerStr = new GridRelation().toAISRelationStr();
            out.write(headerStr);
            out.write("\r\n");
            Iterator iterators = aisRelationMap.keySet().iterator();
            while (iterators.hasNext()) {
                GridRelation gridRelation = aisRelationMap.get(iterators.next());
                String str = null;
                str = gridRelation.toAISRelationSQLString();
                out.write(str);
                out.write("\r\n");
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    //获取当前执行整点字符串
    public String getTimeStr(LocalDateTime dateTime) {
        DateTimeFormatter fomatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime ld = LocalDateTime.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDayOfMonth(), dateTime.getHour(), 0, 0);
        return ld.format(fomatter);
    }

    /**
     * 根据当前时间点的数据，去循环计算每个网格包含多少艘船，计算密度，并且插入或者更新关联表
     *
     * @param dataList    网格船舶关联数据
     * @param dateTimeStr 数据时间段
     */
    public void calcAndOperateRelationWithDB(List<Map> dataList, String dateTimeStr) {
        log.info("开始进行密度计算...");
        long begin = System.currentTimeMillis();
        JSONArray array = new JSONArray();
        dataList.stream().forEach(d -> {
            String gid = d.get("gid").toString();
            String len = d.get("length").toString();
            String area = d.get("area").toString();
            String gridtype = d.get("gridtype").toString();
            String prefecture = d.get("prefecture").toString();
            //进行数据添加
            JSONObject jsonObject = new JSONObject(true);
            jsonObject.put("date", dateTimeStr);
            jsonObject.put("gid", gid);
            jsonObject.put("count", calcShipCountValue(len));
            jsonObject.put("area", area);
            jsonObject.put("gridtype", gridtype);
            jsonObject.put("prefecture", prefecture);
            array.add(jsonObject);
        });
        //记录每个网格的交通密度
        Map<String, GridRelation> dataMap = new ConcurrentHashMap<>();

        List<JsonData> list = JSONObject.parseArray(array.toJSONString(), JsonData.class);
        List<JsonData> newList = new ArrayList<>();
        list.stream()
                .collect(Collectors
                        .groupingBy(
                                json -> new JsonData(json.date, json.gid, json.area, json.gridtype, json.prefecture),
                                Collectors.summarizingDouble(json -> json.count)
                        )
                )
                .forEach((k, v) -> {
                    k.count = v.getSum();
                    newList.add(k);
                    //System.out.println(k);
                });
        dataMap = newList.stream().collect(Collectors.toMap(JsonData::getGridtype, jsonData -> {
                    String date = jsonData.getDate();
                    date = date.substring(0, date.indexOf(" "));
                    GridRelation gridRelation = new GridRelation();
                    gridRelation.setDatatime(date);
                    gridRelation.setGridlevel(jsonData.getGridtype());
                    JSONArray jsonArray = new JSONArray();
                    JSONObject object = new JSONObject(true);
                    object.put("date", jsonData.getDate());
                    object.put("gid", jsonData.getGid());
                    //object.put("prefecture", jsonData.getPrefecture());
                    object.put("prefecture", JSONUtil.encodeStr(jsonData.getPrefecture()));
                    object.put("midu", calcShipMIDUValue(jsonData.getArea(), jsonData.getCount()));
                    object.put("num", calcShipRiskValue(calcShipMIDUValue(jsonData.getArea(), jsonData.getCount())));
                    jsonArray.add(object);
                    gridRelation.setJsondata(jsonArray);
                    return gridRelation;
                }, (GridRelation gridRelation1, GridRelation gridRelation2) -> {
                    //重复的时候
                    gridRelation1.getJsondata().addAll(gridRelation2.getJsondata());
                    return gridRelation1;
                }
        ));
        long end = System.currentTimeMillis();
        log.info("船舶密度计算完成，用时：{}", end - begin);
        //2.写出文件，插入临时表，更新或者插入关联表
        copyToAISRelationDB(dataMap);
    }

    /**
     * 换算船舶数量
     *
     * @param length
     * @return
     */
    public String calcShipCountValue(String length) {
        //换算船舶数量
        double count = 0;
        String risk = "";
        if (length.equals("") || length.equals("\\N")) {//没有length，count直接为1
            count = 1;
        } else {
            double l = Double.parseDouble(length);
            if (l >= 250) {
                count = 3;
            } else if (l >= 200 && l < 250) {
                count = 2.5;
            } else if (l >= 150 && l < 200) {
                count = 2;
            } else if (l >= 100 && l < 150) {
                count = 1.5;
            } else if (l >= 50 && l < 100) {
                count = 1;
            } else {
                count = 0.5;
            }
        }
        return String.valueOf(count);
    }


    /**
     * 计算船舶交通密度
     *
     * @param area
     * @param count
     * @return
     */
    public double calcShipMIDUValue(String area, double count) {
        BigDecimal bCount = new BigDecimal(String.valueOf(count));
        BigDecimal bArea = new BigDecimal(area);
        return bCount.divide(bArea, 2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 计算船舶风险值
     *
     * @param count
     * @return
     */
    public String calcShipRiskValue(double count) {
        String risk = "";
        if (count >= 20) {
            risk = "100";
        } else if (count >= 10 && count < 20) {
            //f(x)=2X+60;
            BigDecimal b1 = new BigDecimal(String.valueOf(count));
            //乘数
            BigDecimal b2 = new BigDecimal("2");
            //加数
            BigDecimal b3 = new BigDecimal("60");
            risk = String.valueOf(b1.multiply(b2).add(b3).doubleValue());
        } else if (count >= 6 && count < 10) {
            //f(x)=2X+60;
            BigDecimal b1 = new BigDecimal(String.valueOf(count));
            //乘数
            BigDecimal b2 = new BigDecimal("5");
            //加数
            BigDecimal b3 = new BigDecimal("30");
            risk = String.valueOf(b1.multiply(b2).add(b3).doubleValue());
        } else if (count >= 0 && count < 6) {
            //f(x)=10X;
            BigDecimal b1 = new BigDecimal(String.valueOf(count));
            //乘数
            BigDecimal b2 = new BigDecimal("10");
            risk = String.valueOf(b1.multiply(b2).doubleValue());
        }
        return risk;
    }

    //从httpclient接收数据以后进行文件写入操作
    public void createJSONFile(String response) {
        try {
            log.info("开始写入文件...");
            long begin = System.currentTimeMillis();
            String content = response;
            //没有文件夹则创建文件夹
            File folder = new File(filepath);
            if (!folder.exists()) {
                folder.mkdir();
            }
            //写入文件
            File file = new File(filepath + "aisData.json");
            file.delete();
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fileWriter);
            bw.write(content);
            bw.close();
            long end = System.currentTimeMillis();
            log.info("写入文件完成，用时：{}", end - begin);
        } catch (IOException e) {
            log.info("写入文件异常，原因：{}", e.getMessage());
        }
    }

    //查看数据表是否存在
    public int existTable(String tableName) {
        return aisRestMapper.existTable(tableName);
    }

    /**
     * 根据httpclient进行数据请求和访问
     *
     * @param httpclienturl httpclient路径
     * @param key           apikey
     * @param lbStr         lb坐标
     * @param rtStr         rt坐标
     */
    private void getAisData(String httpclienturl, String key, String lbStr, String rtStr) {

        //String url = "xx配置";
        String url = httpclienturl;

        // cmd 参数值
        String cmd5110 = "0x5110";

        //String lb = "111.325,20.268", rt = "124.269,39.114";
        String lb = lbStr, rt = rtStr;

        StringBuilder sb = new StringBuilder();
        sb.append("{\"lb\":\"" + lb + "\",\"rt\":\"" + rt + "\"}");

        //String param = Base64.encode(sb.toString().getBytes());
        String param = Base64.getEncoder().encodeToString(sb.toString().getBytes());


        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url + "?cmd=" + cmd5110 + "&param=" + param);

        //String apiKey = "xx配置";
        String apiKey = key;

        httpGet.addHeader("apikey", apiKey);

        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                // 获取数据
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                // 解析数据
                createJSONFile(content);
            }
            response.close();
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 插入热力图数据
     *
     * @param dateTimeStr
     */
    public void insertHeatMapData(String dateTimeStr) {
        //yyyy-MM-dd HH:mm:ss
        try {
            long beginTime = System.currentTimeMillis();
            log.info("开始插入热力数据...");
            GeometryFactory geometryFactory = new GeometryFactory();
            String datatime = dateTimeStr.substring(0, dateTimeStr.indexOf(" "));
            String tablename = "ais_trail_" + datatime.replace("-", "_");
            log.info("tablename:{},time:{}", tablename, dateTimeStr);
            List<Map> data = aisRestMapper.getAisTrailPoint(tablename, dateTimeStr);
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
            aisRestMapper.insertData(dateTimeStr, geomStr, riskGeomStr);
            long endTime = System.currentTimeMillis();
            log.info("数据插入成功,用时:{}", endTime - beginTime);
        } catch (Exception e) {
            log.info("数据插入失败，原因：{}", e.getMessage());
        }
    }
}
