package pers.shezm.calcite.optimizer.converter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import pers.shezm.calcite.optimizer.reloperators.CSVRel;
import pers.shezm.calcite.optimizer.reloperators.CSVTableScan;

public class CSVTableScanConverter extends ConverterRule {

    public static final CSVTableScanConverter INSTANCE = new CSVTableScanConverter(
            LogicalTableScan.class,
            Convention.NONE,
            CSVRel.CONVENTION,
            "CSVTableScan"
    );

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    public CSVTableScanConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
        super(clazz, in, out, description);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan tableScan = (LogicalTableScan) rel;
        return new CSVTableScan(tableScan.getCluster(),
                RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                tableScan.getTable());
    }
}
