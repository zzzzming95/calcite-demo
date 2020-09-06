package pers.shezm.calcite.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CalciteUtil {
    /**
     * 根据给定的 model.json 文件获取 Connection
     *
     * @param filePath
     * @return
     */
    public static Connection getConnect(String filePath) {
        Connection connection = null;
        try {
            URL url = CalciteUtil.class.getResource(filePath);
            String str = URLDecoder.decode(url.toString(), "UTF-8");
            Properties info = new Properties();
            info.put("model", str.replace("file:", ""));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
//            connection.unwrap(CalciteConnection.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 归集查询后的数据并注入到 List
     * @param resultSet
     * @return
     * @throws Exception
     */
    public static List<Map<String, Object>> getData(ResultSet resultSet) throws Exception {
        List<Map<String, Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {

            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }
}
