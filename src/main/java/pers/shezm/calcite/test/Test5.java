package pers.shezm.calcite.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.sql.SqlExplainLevel;
import pers.shezm.calcite.optimizer.converter.CSVTableScanConverter;
import pers.shezm.calcite.optimizer.cost.DefaultRelMetadataProvider;
import pers.shezm.calcite.utils.CalciteUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 通过 MetadataProvider 的方式，并实现相关的 MetadataHandler，最终实现自己计算 cost 的逻辑
 * 不过需要将 RelNode 转换成自己实现的 RelNode，才能实现注入（比如CSVTableScan）
 */
public class Test5 {
    public static void main(String[] args)  {
        String sql = "select * from TEST_CSV.TEST01 where TEST01.NAME1='hello'";

        String filePath = "/model.json";
        Connection connection = null;
        try {
            connection = CalciteUtil.getConnect(filePath);
            RelRoot root = Test2.genRelRoot(connection, sql);
            System.out.println("----------------- before optimizer ------------------");
            System.out.println(RelOptUtil.toString(root.rel, SqlExplainLevel.ALL_ATTRIBUTES));

            DefaultRelMetadataProvider defaultRelMetadataProvider = new DefaultRelMetadataProvider();
            defaultRelMetadataProvider.getMetadataProvider();
            RelNode rel = Test5.hepPlan(root.rel,false,defaultRelMetadataProvider.getMetadataProvider(),null,null, CSVTableScanConverter.INSTANCE);

            System.out.println("----------------- after optimizer ------------------");
            /**这里修改了 TableScan 到 Filter 的 rowcount 的计算逻辑，
             * 详见 {@link pers.shezm.calcite.optimizer.cost.CSVRelMdRowCount#getRowCount(Filter rel, RelMetadataQuery mq) }*/
            System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
    }


    /**
     * Run the HEP Planner with the given rule set.
     *
     * @param basePlan
     * @param followPlanChanges
     * @param mdProvider
     * @param executorProvider
     * @param order
     * @param rules
     * @return optimized RelNode
     */
    public static RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
                            RelMetadataProvider mdProvider, RelOptPlanner.Executor executorProvider, HepMatchOrder order,
                            RelOptRule... rules) {

        RelNode optimizedRelNode = basePlan;
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        if (followPlanChanges) {
            programBuilder.addMatchOrder(order);
            programBuilder = programBuilder.addRuleCollection(ImmutableList.copyOf(rules));
        } else {
            // TODO: Should this be also TOP_DOWN?
            for (RelOptRule r : rules)
                programBuilder.addRuleInstance(r);
        }

        // Create planner and copy context
        HepPlanner planner = new HepPlanner(programBuilder.build(),
                basePlan.getCluster().getPlanner().getContext());

        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(mdProvider);
        planner.registerMetadataProviders(list);
        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
        basePlan.getCluster().setMetadataProvider(
                new CachingRelMetadataProvider(chainedProvider, planner));

        if (executorProvider != null) {
            basePlan.getCluster().getPlanner().setExecutor(executorProvider);
        }
        planner.setRoot(basePlan);
        optimizedRelNode = planner.findBestExp();

        return optimizedRelNode;
    }
}
