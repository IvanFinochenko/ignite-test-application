package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class SqlScripts {
    private static final Map<String, String> sqlScripts = new HashMap<>();

    static  {
        String directory = "src/main/resources/sql";
        File sqlDir = new File(directory);
        readScripts(sqlDir);
    }


    private static void readScripts(File sqlDir) {
        File[] scripts = sqlDir.listFiles();
        StringBuilder sql = new StringBuilder();
        String line;
        if (scripts == null) return;
        for (File file: scripts) {
            if (file.isDirectory()) {
                readScripts(file);
            } else {
                try (BufferedReader br = new BufferedReader(new FileReader(file.getPath()))) {
                    line = br.readLine();
                    while (line != null) {
                        sql.append(line).append(" ");
                        line = br.readLine();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                sqlScripts.put(file.getName().split("\\.")[0], sql.toString());
                sql.setLength(0);
            }
        }
    }

    public static String getSql(String name) {
        return sqlScripts.get(name);
    }
}
