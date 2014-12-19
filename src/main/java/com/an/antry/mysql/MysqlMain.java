package com.an.antry.mysql;

import java.io.IOException;
import java.sql.SQLException;

public class MysqlMain {
    /**
     * @param args
     */
    public static void main(String[] args) {
        DbOper oper = null;
        try {
            oper = new DbOper();
            // oper.testInit();
            // oper.query();
            // oper.createFile();
            oper.createFile2();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            oper.closeConn();
        }
    }
}
