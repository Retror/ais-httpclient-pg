package com.hdsx.osp.ais.util;

import org.apache.http.HttpStatus;

import java.util.List;

public class ResultJson<T> {

    private int code;
    private String msg;
    private T data;
    // 页数
    private Integer page;

    // 条数
    private Integer total;
    // 结果数组
    private List rows;

    //分页构造
    public ResultJson(List rows, Integer page, Integer total) {
        super();
        this.rows = rows;
        this.page = page;
        this.total = total;
        this.code = 200;
        this.msg = "success";
    }
    public static <T> ResultJson<T> build(List rows, Integer page, Integer total){
        return new ResultJson<T>(rows, page, total);
    }

    public static <T> ResultJson<T> error(String msg) {
        return error(HttpStatus.SC_INTERNAL_SERVER_ERROR, msg);
    }

    // 成功调用
    public static <T> ResultJson<T> success(T data) {
        return new ResultJson<T>(data);
    }
    // 失败调用 支持 e.getcode,e.getmessage 传递参数
    public static <T> ResultJson<T> error(int code,String msg) {
        return new ResultJson<T>(code,msg);
    }
    public static <T> ResultJson<T> error(CodeMsg codeMsg) {
        return new ResultJson<T>(codeMsg);
    }

    private ResultJson(T data) {
        this.code = 200;
        this.msg = "success";
        this.data = data;
    }
    public ResultJson(){

    }

    public ResultJson(int code, String msg) {
        this.code = code;
        this.msg = msg;
        data = (T)"";
    }
    public ResultJson(CodeMsg cm) {
        if (cm == null) {
            return;
        }
        this.code = cm.getCode();
        this.msg = cm.getMsg();
        data = (T)"";
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public T getData() {
        return data;
    }

    public List getRows() {
        return rows;
    }

    public void setRows(List rows) {
        this.rows = rows;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }
}