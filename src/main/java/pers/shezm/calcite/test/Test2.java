package pers.shezm.calcite.test;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import pers.shezm.calcite.utils.CalciteUtil;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * 使用自定义的 csv 源，解析校验 SQL ，遍历 RelNode 树，统计其中一些信息并打印
 */
public class Test2 {

    public static void main(String[] args)  {
        String sql = "select * from TEST_CSV.TEST01 as t1 left join TEST_CSV.TEST02 as t2 on t1.NAME1=t2.NAME3";
        String filePath = "/model.json";
        Connection connection = null;
        try {
            connection = CalciteUtil.getConnect(filePath);
            RelRoot root = Test2.genRelRoot(connection, sql);
            Test2.printSqlInfo(root.rel);
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
     * Planner解析，校验，然后生成RelNode，使用mysql的sql语法格式
     *
     * @param connection
     * @param sql
     * @return
     * @throws Exception 参考自：https://zhuanlan.zhihu.com/p/65345335
     */
    public static RelRoot genRelRoot(Connection connection, String sql) throws Exception {
        //从 conn 中获取相关的环境和配置，生成对应配置
        CalciteServerStatement st = connection.createStatement().unwrap(CalciteServerStatement.class);
        CalcitePrepare.Context prepareContext = st.createPrepareContext();
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(prepareContext.getRootSchema().plus())
//                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();
        Planner planner = Frameworks.getPlanner(config);
        RelRoot root = null;
        try {
            SqlNode parse1 = planner.parse(sql);
            SqlNode validate = planner.validate(parse1);
            root = planner.rel(validate);
            RelNode rel = root.rel;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return root;
    }

    /**
     * 遍历 RelNode 并打印一些节点统计信息
     *
     * @param rel
     */
    public static void printSqlInfo(RelNode rel) {
        Queue<RelNode> relNodeQueue = new LinkedList<RelNode>();
        relNodeQueue.offer(rel);
        /**
         * RelNode类型：
         * TableScan（获取表信息，列信息）
         * 一元节点：
         * LogicalJoin
         * Sort
         * GROUP BY
         * ......
         *
         * 二元信息：
         * LogicalJoin
         * Union
         * ......
         *
         */
        int joinCount = 0;
        int aggregateCount = 0;
        //层次遍历树并获取信息
        while (relNodeQueue.size() != 0) {
            int inputNum = relNodeQueue.size();
            for (int i = 0; i < inputNum; i++) {
                RelNode tem = relNodeQueue.poll();
                for (RelNode r : tem.getInputs()) {
                    relNodeQueue.offer(r);
                }
                if (tem.getRelTypeName().contains("Join")) {
                    joinCount += 1;
                }
                if (tem.getRelTypeName().contains("Aggregate")) {
                    aggregateCount += 1;
                }
                //print table info
                if (tem.getTable() != null) {
                    RelOptTable rtable = tem.getTable();
                    System.out.println("------------------ table " + rtable.getQualifiedName() + " scan info: ------------------");
                    System.out.println("row name and type : " + rtable.getRowType());
                    System.out.println("distribution info : " + rtable.getDistribution());  //由 RelDistribution 的类型决定
                    System.out.println("columns strategies : " + rtable.getColumnStrategies());
                    System.out.println("------------------end table " + rtable.getQualifiedName() + " ------------------");
                }
//                RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
            }
        }
        //print sql info
        System.out.println("Join num is : " + joinCount);
        System.out.println("Aggregate num is : " + joinCount);

//        System.out.println("After------------------");

    }

}
