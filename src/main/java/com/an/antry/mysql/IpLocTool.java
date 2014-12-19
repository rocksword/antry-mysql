package com.an.antry.mysql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool used to generate IP location files. <br>
 * Usage: java -Xmx4096m -jar iplocation-0.0.1.jar {DB host} {Data table} {username} {password} {file store directory}
 * 
 * @author adong
 */
public class IpLocTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(IpLocTool.class);
    private static final long LINE_NUM_EACH_FILE = 10000000L;
    private static final String ERROR_MSG = "Require five arguments: DB_HOST, DB_NAME, USERNAME, PASSWORD, FILE_STORE_DIR.";
    private static String host = null;
    private static String dbname = null;
    private static String username = null;
    private static String password = null;
    private static String fileDir = null;

    private Connection conn = null;

    public static void main(String[] args) {
        if (args.length != 5) {
            LOGGER.error("{}", ERROR_MSG);
            System.exit(1);
        }

        host = args[0];
        dbname = args[1];
        username = args[2];
        password = args[3];
        fileDir = args[4];
        if (host.trim().isEmpty() || dbname.trim().isEmpty() || username.trim().isEmpty() || fileDir.trim().isEmpty()) {
            LOGGER.error("Illegal arguments, host: {}, db name: {}, username: {}, file dir: {}", host, dbname,
                    username, fileDir);
            System.exit(1);
        }

        IpLocTool tool = new IpLocTool();
        try {
            tool.init();
            tool.createFile();
        } catch (SQLException | IOException | ClassNotFoundException e) {
            LOGGER.error("Error ", e);
        } finally {
            tool.closeConn();
        }
    }

    public void createFile() throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            String sql = "SELECT startIpNum, endIpNum, latitude, longitude FROM blockslocation;";
            rs = st.executeQuery(sql);

            StringBuilder content = new StringBuilder();

            long lastFinishedIpNum = 0L;
            int fileNameSuffix = 0;
            long lineNum = 0L;
            while (rs.next()) {
                long startIpNum = rs.getLong("startIpNum");
                long endIpNum = rs.getLong("endIpNum");
                double latitude = rs.getDouble("latitude");
                double longitude = rs.getDouble("longitude");

                for (long ipl = startIpNum; ipl <= endIpNum; ipl++) {

                    if (ipl <= lastFinishedIpNum) {
                        continue;
                    }

                    lineNum++;

                    content.append(ipl).append(",").append(latitude).append(",").append(longitude).append("\n");
                    if (lineNum >= LINE_NUM_EACH_FILE) {

                        String fp = String.format(fileDir + File.separator + "iploc_%s.csv", ++fileNameSuffix);
                        writeFile(fp, content.toString());

                        // Reset local variable
                        lineNum = 0;
                        content.setLength(0);

                        pause();
                    }
                }
            }

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

    private void writeFile(String filePath, String content) throws IOException {
        if (content == null || content.isEmpty()) {
            LOGGER.warn("Empty content.");
            return;
        }

        File file = new File(filePath);
        if (file.exists()) {
            LOGGER.warn("File exists, {}", filePath);
            return;
        }

        LOGGER.info("Create file {}", filePath);
        file.createNewFile();
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(filePath))) {
            bf.write(content);
        }
    }

    private void init() throws ClassNotFoundException, SQLException {
        LOGGER.info("Init.");
        Class.forName("org.gjt.mm.mysql.Driver");
        String url = String.format("jdbc:mysql://%s:3306/%s", host, dbname);
        LOGGER.info("Get connection {}", url);
        conn = DriverManager.getConnection(url, username, password);
    }

    private void closeConn() {
        if (conn != null) {
            LOGGER.info("Close conn.");
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("Error ", e);
            }
        }
    }

    private void pause() {
        try {
            Thread.sleep(2 * 1000L);
        } catch (InterruptedException e) {
            LOGGER.error("Error ", e);
        }
    }
}
