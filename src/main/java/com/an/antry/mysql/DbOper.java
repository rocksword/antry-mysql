package com.an.antry.mysql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DbOper {
    private String host = "10.103.218.27";
    private String dbname = "geoloc";
    private String username = "root";
    private String password = "root";
    private Connection conn = null;
    private String filePath1 = "D:\\data\\ipgeo_%s.txt";
    private String filePath2 = "/home/hadoop/adong/ipgeo/ipgeo_%s.txt";

    public DbOper() {
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createFile() throws SQLException, IOException {
        Statement st = null;
        try {
            st = conn.createStatement();
            long t1 = System.currentTimeMillis();
            String sql = String.format("select startIpNum, endIpNum, latitude, longitude from blockslocation;");
            ResultSet rs = st.executeQuery(sql);
            long i = 0L;
            long num = 10000000L;
            int cnt = 359;
            StringBuilder content = new StringBuilder();
            long finished = 3753599926L;
            while (rs.next()) {
                long start = rs.getLong("startIpNum");
                long end = rs.getLong("endIpNum");
                double lat = rs.getDouble("latitude");
                double lng = rs.getDouble("longitude");
                for (long ipl = start; ipl <= end; ipl++) {
                    if (ipl <= finished) {
                        continue;
                    }
                    i++;
                    content.append(ipl).append(",").append(lat).append(",").append(lng).append("\n");
                    if (i >= num) {
                        File ff = new File(String.format(filePath2, ++cnt));
                        long t2 = System.currentTimeMillis();
                        System.out.println("Time " + (t2 - t1) + " ms.");
                        t1 = t2;
                        writeFile(ff.toString(), content.toString());
                        System.out.println((System.currentTimeMillis() - t2) + " ms, finished write " + ff.toString());
                        content.setLength(0);

                        try {
                            System.out.println("Sleep 2s.");
                            long t11 = System.currentTimeMillis();
                            Thread.sleep(5 * 1000L);
                            System.out.println(System.currentTimeMillis() - t11);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        i = 0;
                    }
                }
            }
            if (rs != null) {
                rs.close();
            }
        } finally {
            st.close();
        }
    }

    public void query() throws SQLException, IOException {
        File f = new File("D:\\github\\antry-mysql\\src\\xbl");
        FileLineIterator iter = new FileLineIterator(f.toString());
        String line = null;
        Statement st = null;
        try {
            st = conn.createStatement();
            int i = 0;
            long t1 = System.currentTimeMillis();
            StringBuilder content = new StringBuilder();
            while ((line = iter.nextLine()) != null) {
                if (line.startsWith("#") || line.startsWith(":")) {
                    continue;
                }
                InetAddress address = InetAddress.getByName(line);
                if (address.isSiteLocalAddress() || address.isLinkLocalAddress() || address.isLoopbackAddress()) {
                    continue;
                }
                String ip = address.getHostAddress();
                long iplong = ip2Long(ip);
                // System.out.println(iplong + ", " + ip);
                String sql = String.format(
                        "select latitude,longitude from blockslocation where startIpNum<%s and endIpNum>%s;", iplong,
                        iplong);
                ResultSet rs = st.executeQuery(sql);
                double lat = 0;
                double lng = 0;
                while (rs.next()) {
                    lat = rs.getDouble("latitude");
                    lng = rs.getDouble("longitude");
                    // System.out.println(lat + " - " + lng);
                }
                if (rs != null) {
                    rs.close();
                }
                content.append(iplong).append(",").append(ip).append(",").append(lat).append(",").append(lng)
                        .append("\n");
                i++;
                int num = 10000;
                if (i % num == 0) {
                    long t2 = System.currentTimeMillis();
                    System.out.println("Time " + (t2 - t1) + " ms.");
                    t1 = t2;
                    File ff = new File(String.format("D:\\data\\geoloc_%s.txt", i / num));
                    writeFile(ff.toString(), content.toString());
                }
            }
        } finally {
            iter.close();
            st.close();
        }
    }

    public void writeFile(String filePath, String content) throws IOException {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Invalid filePath " + filePath);
        }
        if (content == null || content.isEmpty()) {
            System.out.println("Empty content.");
        }

        File file = new File(filePath);
        if (file.exists()) {
            System.out.println("File exists");
            return;
        }
        file.createNewFile();

        System.out.println("Write file " + filePath);

        try (BufferedWriter bf = new BufferedWriter(new FileWriter(filePath))) {
            bf.write(content);
        }
    }

    private long ip2Long(String ip) {
        long result = 0L;
        String[] ipArr = ip.split("\\.");
        for (int i = 3; i >= 0; i--) {
            long ipL = Long.parseLong(ipArr[3 - i]);
            result |= ipL << (i * 8);
        }
        return result;
    }

    public void closeConn() {
        if (conn != null) {
            try {
                System.out.println("Close conn " + conn);
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Connection getConn() {
        return conn;
    }

    public void testInit() {
        System.out.println("conn: " + conn);
    }

    private void init() throws ClassNotFoundException, SQLException {
        System.out.println("Initialize connection.");
        Class.forName("org.gjt.mm.mysql.Driver");
        String url = String.format("jdbc:mysql://%s:3306/%s", host, dbname);
        System.out.println(String.format("Init connection, url: %s, username: %s, password: %s", url, username,
                password));
        conn = DriverManager.getConnection(url, username, password);
    }
}
