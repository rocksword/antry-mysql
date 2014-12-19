package com.an.antry.mysql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtil {
    /**
     * @param filePath
     * @param content
     * @throws IOException
     */
    public static void writeFile(String filePath, String content) throws IOException {
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
}
