package com.hdsx.osp.ais.util;

import com.alibaba.fastjson.JSONReader;
import com.hdsx.osp.ais.bean.AISBase;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * JSON操作工具类
 */
@Slf4j
public class JSONUtil {

    public static List<AISBase> ReadJSONFile(String file) {
        List<AISBase> list = new ArrayList<>();
        //进行json文本数据读取操作
        try {
            JSONReader reader = new JSONReader(new FileReader(file));
            reader.startArray();
            while (reader.hasNext()) {
                AISBase aisBase = reader.readObject(AISBase.class);
                list.add(aisBase);
            }
            reader.endArray();
            reader.close();
        } catch (FileNotFoundException e) {
            log.info("文件读取异常，原因{}", e.getMessage());
        }
        return list;
    }

    //base64编码
    public static String encodeStr(String str) {
        String base64encodedString = "";
        try {
            base64encodedString = Base64.getEncoder().encodeToString(str.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64encodedString;
    }

    //base64解码
    public static String decodeStr(String str) {
        // 解码
        byte[] base64decodedBytes = Base64.getDecoder().decode(str);
        try {
            str = new String(base64decodedBytes, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return str;
    }

    public static void main(String[] args) {
        JSONUtil.ReadJSONFile("F://jsondata//aisData.json");
    }
}
