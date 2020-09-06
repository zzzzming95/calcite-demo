package pers.shezm.calcite.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.net.URL;
import java.util.Map;

public class CsvSchema extends AbstractSchema {
    private Map<String, Table> tableMap;
    private String dataFiles;

    public CsvSchema(String dataFile) {
        this.dataFiles = dataFile;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (String dataFile : dataFiles.split(",")) {
            URL url = Resources.getResource(dataFile);
            Source source = Sources.of(url);
            builder.put(dataFile.split("\\.")[0], new CsvTable(source));
        }
        // 一个数据库有多个表名，这里初始化，大小写要注意了
        tableMap = builder.build();

        return tableMap;
    }
}