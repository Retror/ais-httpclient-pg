package com.hdsx.osp.ais.util;

import org.apache.http.HttpStatus;

/**
 * 返回信息定义
 **/
public class CodeMsg {

    private int code;
    private String msg;

    /**通过静态方法调用CodeMsg对象获取对应error信息 自定义error 信息
     * */
    /**
     * 服务端异常
     */
    public static CodeMsg SUCCESS = new CodeMsg(0,"成功");
    public static CodeMsg SERVER_ERROR = new CodeMsg(500,"未知异常，请联系管理员");
    public static CodeMsg NO_HANDLER_FOUND = new CodeMsg(HttpStatus.SC_NOT_FOUND,"路径不存在，请检查路径是否正确");
    /**
     * token
     */
    public static CodeMsg INVALID_TOKEN = new CodeMsg(HttpStatus.SC_UNAUTHORIZED,"用户Token无效");
    /**
     * 认证异常
     */
    public static CodeMsg NO_AUTHORIZATION = new CodeMsg(HttpStatus.SC_FORBIDDEN,"没有权限，请联系管理员授权");

    public static CodeMsg SYSTEM_EXCEPTION = new CodeMsg(1001,"系统异常");
    public static CodeMsg USER_CHECK = new CodeMsg(1002,"用户验证失败");
    public static CodeMsg PARAMS_ERROR = new CodeMsg(1003,"参数错误");
    public static CodeMsg DATA_ERROR = new CodeMsg(1004,"json参数错误");
    public static CodeMsg ISNULL = new CodeMsg(1005,"参数为空");
    public static CodeMsg PASSWORD_ERROR = new CodeMsg(101,"密码错误");
    public static CodeMsg USERNAME_ERROR = new CodeMsg(102,"账户不存在");

    private CodeMsg(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    /** 该方法用于返回一个CodeMsg对象 便于 全局异常处理的调用
     *  全局异常处理传入 objects 参数，并返回一个CodeMsg 对象
     *  该方法根据入参 显示 对应的异常code , 以及加入 异常信息的msg显示
     *  利用可变长参数定义 ：适用于 参数类型可知，但是个数未知的情况
     * */
    public CodeMsg fillArgs(Object...objects){
        int code = this.code;
        String message = String.format(this.msg,objects);
        return new CodeMsg(code,message);
    }
}
