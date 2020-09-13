package pers.shezm.calcite.optimizer.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import pers.shezm.calcite.optimizer.reloperators.CSVProject;
import pers.shezm.calcite.optimizer.reloperators.CSVTableScan;
import pers.shezm.calcite.optimizer.reloperators.NewCsvProject;

public class CSVRelMdRowCount extends RelMdRowCount {
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
            .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new CSVRelMdRowCount());

    @Override
    public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
        return 1.0;
    }

    //以下两个方法用于测试 CBO 优化
    public Double getRowCount(NewCsvProject rel, RelMetadataQuery mq) {
        return 1.0;
    }

    public Double getRowCount(CSVProject rel, RelMetadataQuery mq) {
        return 1.0;
    }

    public Double getRowCount(Project rel, RelMetadataQuery mq) {
        return 2.0;
    }

    //实现自己的 cost 计算逻辑
    @Override
    public Double getRowCount(Filter rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInput()) / 5;
//        return 5.0;
    }


    public Double getRowCount(RelNode rel) {
        return 1.0;
    }

    public Double getRowCount(RelNode rel,RexNode rex,RelMetadataQuery mq) {
        return 1.0;
    }
}
