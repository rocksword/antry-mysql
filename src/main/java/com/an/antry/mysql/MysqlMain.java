package com.an.antry.mysql;

import java.io.IOException;
import java.sql.SQLException;

public class MysqlMain {
    public static void main(String[] args) {
        try {
            run();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void run() throws SQLException, IOException {
        DbOper oper = new DbOper();
        // oper.testInit();
        oper.query();
        oper.closeConn();
    }
}
