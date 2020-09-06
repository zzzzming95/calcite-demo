package pers.shezm.calcite.test;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.sql.SqlExplainLevel;
import pers.shezm.calcite.utils.CalciteUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 打印 SQL 的 Cost 信息，添加 Calcite 的优化器后，RelNode 的信息
 */
public class Test3 {
    public static void main(String[] args)  {
        String sql = "select * from TEST_CSV.TEST01 as t1 left join TEST_CSV.TEST02 as t2 " +
                "on t1.NAME1=t2.NAME3 " +
                "where t1.NAME1='hello'";
        String filePath = "/model.json";
        Connection connection = null;
        try {
            connection = CalciteUtil.getConnect(filePath);
            RelRoot root = Test2.genRelRoot(connection, sql);
            Test3.optimize(root.rel);
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
     * 构建一个优化器，添加 FILTER_ON_JOIN 这个优化 rule，然后进行优化，打印优化前后对比，以及对应的 cost 信息
     * 在 Calcite 中，树上层的 Cost 信息都是通过下层的信息计算而来，所以最重要的就是底层 TableSacn 的 Cost 信息
     * 而默认的 rowcount 是 100，CPU 是 101
     * 可寻迹 Calcite 源码： TableScan.computeSelfCost(xxx) -> RelOptAbstractTable.getRowCount() 查看
     * @param rel
     * @return
     */
    public static RelNode optimize(RelNode rel){
        System.out.println("----------------- before optimizer ------------------");
        System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN); //note: 添加 rule
        HepPlanner hepPlanner = new HepPlanner(builder.build());  //同时也可以仿照 FilterIntoJoinRule 这个类实现自己的优化 rule
        hepPlanner.setRoot(rel);
        rel = hepPlanner.findBestExp();
        System.out.println("----------------- after optimizer ------------------");

        System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

        return rel;
    }
}
