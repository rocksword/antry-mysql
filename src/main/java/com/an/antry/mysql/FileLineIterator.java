package com.an.antry.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileLineIterator {
    private String filepath;
    private BufferedReader br;

    /**
     * @param filepath
     *            absolute path
     */
    public FileLineIterator(String filepath) {
        this.filepath = filepath;
    }

    public String nextLine() {
        String line = null;
        try {
            if (br == null) {
                File file = new File(this.filepath);
                FileReader fr = new FileReader(file);
                br = new BufferedReader(fr);
            }
            line = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (line == null) {
            close();
        }
        return line;
    }

    public void close() {
        if (br != null) {
            try {
                System.out.println("Close BufferedReader: " + br);
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
