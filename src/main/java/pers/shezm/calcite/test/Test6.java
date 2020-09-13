package pers.shezm.calcite.test;

import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.*;
import pers.shezm.calcite.optimizer.converter.*;
import pers.shezm.calcite.optimizer.cost.DefaultRelMetadataProvider;
import pers.shezm.calcite.utils.CalciteUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.apache.calcite.rel.core.RelFactories.DEFAULT_STRUCT;

/**
 * 对比 RBO 和 CBO，在 Calcite 中，对应的是 HepPlanner 和 VolcanoPlanner。
 * 遇到多个可匹配 rule 的时候，HepPlanner 会按照顺序进行匹配生成 RelNode。而 VolcanoPlanner 会根据最终的 Cost 生成 RelNode。
 */
public class Test6 {
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
            RelNode rel = Test5.hepPlan(root.rel,
                    false,
                    defaultRelMetadataProvider.getMetadataProvider(),
                    null,null,
                    CSVTableScanConverter.INSTANCE,
                    CSVFilterConverter.INSTANCE,
                    CSVProjectConverter.INSTANCE,
                    CSVNewProjectConverter.INSTANCE);

            System.out.println("----------------- after RBO optimizer 1------------------");
            System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

            //将最后两个 rule 改变一下顺序，会发现结果的顺序也改变了，说明 RBO 只会简单得遍历 rule 然后应用
            RelNode rel1 = Test5.hepPlan(root.rel,
                    false,
                    defaultRelMetadataProvider.getMetadataProvider(),
                    null,null,
                    CSVTableScanConverter.INSTANCE,
                    CSVFilterConverter.INSTANCE,
                    CSVNewProjectConverter.INSTANCE,
                    CSVProjectConverter.INSTANCE);
            System.out.println("----------------- after RBO optimizer 2------------------");
            System.out.println(RelOptUtil.toString(rel1, SqlExplainLevel.ALL_ATTRIBUTES));

            //这里的 rule 是替换 CsvProject 为 NewCsvProject，是否替换会根据 cumulative cost 的信息，谁的小就替换谁的
            //我直接在对应的 rel 里面写死了返回的 cost 信息（rows:10,cpu:10,io:0），如果调高一点（高过 CsvProject 的定义），那么是不会替换的
            rel = CBOOptimizer(rel,
                    CSVNewProjectRule.INSTANCE
            );
            System.out.println("----------------- after CBO optimizer ------------------");
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


    public static class FilterIntoJoinRule extends FilterJoinRule {
        public FilterIntoJoinRule(boolean smart,
                                  RelBuilderFactory relBuilderFactory, Predicate predicate) {
            super(
                    operand(Filter.class,
                            operand(Join.class, RelOptRule.any())),
                    "FilterJoinRule:filter", smart, relBuilderFactory,
                    predicate);
        }

        @Deprecated // to be removed before 2.0
        public FilterIntoJoinRule(boolean smart,
                                  RelFactories.FilterFactory filterFactory,
                                  RelFactories.ProjectFactory projectFactory,
                                  Predicate predicate) {
            this(smart, RelBuilder.proto(filterFactory, projectFactory), predicate);
        }

        @Override public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            Join join = call.rel(1);
            perform(call, filter, join);
        }
    }

    public static RelNode CBOOptimizer(RelNode rel, RelOptRule... rules){
//        rel.getCluster()
        VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
        //VolcanoPlanner 默认带有很多的优化 rule，其中有一个 ProjectRemoveRule 会消除掉 Project，故先 clear
        planner.clear();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        //由于火山模型用的 RelMetadataProvider 是 VolcanoRelMetadataProvider ，所以哪怕这里注入了我们自定义的 RelMetadataProvider 也不会生效
//        List<RelMetadataProvider> list = Lists.newArrayList();
//        DefaultRelMetadataProvider mdProvider = new DefaultRelMetadataProvider();
//        list.add(mdProvider.getMetadataProvider());
//        planner.registerMetadataProviders(list);
//        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
//        rel.getCluster().setMetadataProvider(
//                new CachingRelMetadataProvider(chainedProvider, planner));

        for (RelOptRule r : rules)
            planner.addRule(r);

        RelOptCluster cluster = newCluster(planner);

        cluster.getPlanner().setRoot(rel);
        RelNode result = planner.chooseDelegate().findBestExp();
        return result;
    }

    static RelOptCluster newCluster(VolcanoPlanner planner) {
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    public static RelRoot genRelRootWithVolcanoPlanner(Connection connection, String sql) throws Exception {
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

        //添加 VolcanoPlanner 优化器
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        RelOptCluster cluster = newCluster(volcanoPlanner);
        RelBuilderFactory LOGICAL_BUILDER =
                RelBuilder.proto(Contexts.of(DEFAULT_STRUCT));
        final RelBuilder relBuilder = LOGICAL_BUILDER.create(cluster, null);

        try {
            SqlNode parse1 = planner.parse(sql);
            SqlNode validate = planner.validate(parse1);
            root = planner.rel(validate);
            RelNode rel = root.rel;
        } catch (Exception e) {
            e.printStackTrace();
        }
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        return root;
    }
}
