package pers.shezm.calcite.test;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlExplainLevel;
import pers.shezm.calcite.optimizer.converter.CSVFilterConverter;
import pers.shezm.calcite.optimizer.converter.CSVNewProjectConverter;
import pers.shezm.calcite.optimizer.converter.CSVProjectConverter;
import pers.shezm.calcite.optimizer.converter.CSVTableScanConverter;
import pers.shezm.calcite.utils.CalciteUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 要实现优化器，可能需要 cost 信息，包括 rowcount，cpu，io 等，Calcite 默认实现比较粗糙
 * 且有些组件会维护自己的元数据信息（比如 hive），那么就需要修改底层的 RelNode 的 Cost 计算逻辑，注入自己的元数据信息
 * 有两种方式，这里介绍第一种，直接转换成自定义的 RelNode
 */
public class Test4 {
    public static void main(String[] args)  {
        String sql = "select * from TEST_CSV.TEST01 as t1 left join TEST_CSV.TEST02 as t2 " +
                "on t1.NAME1=t2.NAME3 " +
                "where t1.NAME1='hello'";
        sql = "select * from TEST_CSV.TEST01 where TEST01.NAME1='hello'";

        String filePath = "/model.json";
        Connection connection = null;
        try {
            connection = CalciteUtil.getConnect(filePath);
            RelRoot root = Test2.genRelRoot(connection, sql);
            Test4.optimize(root.rel);
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

    /** Calcite RBO 优化器执行顺序
     * 先按照 rule 的顺序（比如我这里有三个 converter），每条 rule 都会去 applyrules。
     *
     * applyrules就会去遍历整颗 Rel 树（每个都会包装成HepRelVertex），每个节点都尝试match，并且是自上而下match。match 了就会生成一个HepRuleCall。然后会 apply对应的 transform。
     * @param rel
     * @return
     */
    public static RelNode optimize(RelNode rel){
        System.out.println("----------------- before optimizer ------------------");
        System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(CSVTableScanConverter.INSTANCE);
        builder.addRuleInstance(CSVFilterConverter.INSTANCE);
        builder.addRuleInstance(CSVProjectConverter.INSTANCE);

        HepPlanner hepPlanner = new HepPlanner(builder.build());  //同时也可以仿照 FilterIntoJoinRule 这个类实现自己的优化 rule
        hepPlanner.setRoot(rel);
        rel = hepPlanner.findBestExp();
        System.out.println("----------------- after optimizer ------------------");

        System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

        return rel;
    }



}
