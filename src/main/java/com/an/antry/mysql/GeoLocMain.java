package com.an.antry.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoLocMain {
    private static String geoLoc = "D:\\data\\Geo-Location.csv";
    private static String geoBlock = "D:\\data\\GeoIPCity-Blocks.csv";

    public static void main(String[] args) throws IOException {
        // handleBlocks();
        // handleLoc();
        createBlockFile();
    }

    private static void createBlockFile() throws IOException {
        FileLineIterator it = new FileLineIterator(geoBlock);
        StringBuilder cont = new StringBuilder();
        String line = null;
        while ((line = it.nextLine()) != null) {
            String[] strs = line.split(",");
            if (strs.length == 3) {
                cont.append(String.format("%010d", Long.parseLong(strs[0]))).append(",")
                        .append(String.format("%010d", Long.parseLong(strs[1]))).append(",").append(strs[2])
                        .append("\n");
            }
        }
        it.close();
        FileUtil.writeFile("D:\\data\\geoloc.csv", cont.toString());
    }

    private static void handleBlocks() {
        FileLineIterator it = new FileLineIterator(geoBlock);
        String line = null;
        Map<Integer, List<IpRange>> blockMap = new HashMap<Integer, List<IpRange>>();
        while ((line = it.nextLine()) != null) {
            String[] strs = line.split(",");
            int locId = Integer.parseInt(strs[2]);
            IpRange ir = new IpRange(Long.parseLong(strs[0]), Long.parseLong(strs[1]));
            if (!blockMap.containsKey(locId)) {
                blockMap.put(locId, new ArrayList<IpRange>());
            }
            blockMap.get(locId).add(ir);
        }
        it.close();

        List<Integer> locIdList = new ArrayList<Integer>();
        locIdList.addAll(blockMap.keySet());
        System.out.println("Loc Id count: " + locIdList.size());
        for (int locId : locIdList) {
            // System.out.println(locId + " - " + blockMap.get(locId));
            List<IpRange> list = blockMap.get(locId);
            Collections.sort(list);
            List<IpRange> newList = new ArrayList<IpRange>();
            for (int i = 0; i < list.size() - 1; i++) {
                IpRange ir1 = list.get(i);
                IpRange ir2 = list.get(i + 1);
                if (ir2.getStartIp() - ir1.getEndIp() == 1) {
                    IpRange newIr = new IpRange(ir1.getStartIp(), ir2.getEndIp());
                    System.out.println("Merger " + ir1 + ',' + ir2 + ", locId: " + locId);
                }
            }
        }
    }

    private static void handleLoc() throws IOException {
        String filepath = "D:\\data\\GeoIPCity-Location.csv";
        FileLineIterator it = new FileLineIterator(filepath);
        String line = null;
        Map<Integer, List<IpRange>> blockMap = new HashMap<Integer, List<IpRange>>();
        StringBuilder cont = new StringBuilder();
        while ((line = it.nextLine()) != null) {
            // 140,"ML","","","",17.0000,-4.0000,,
            String[] strs = line.split(",");
            if (strs.length >= 7) {
                try {
                    int locId = Integer.parseInt(strs[0]);
                    float lat = Float.parseFloat(strs[5]);
                    float lng = Float.parseFloat(strs[6]);
                    cont.append(locId).append(",").append(lat).append(",").append(lng).append("\n");
                } catch (Exception e) {
                    System.out.println(line);
                    e.printStackTrace();
                }
            }
        }
        it.close();

        FileUtil.writeFile(geoLoc, cont.toString());
    }
}
