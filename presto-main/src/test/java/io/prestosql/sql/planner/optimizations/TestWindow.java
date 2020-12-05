/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRankingNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static java.lang.String.format;

public class TestWindow
        extends BasePlanTest
{
    @Test
    public void testWindow()
    {
        // Window partition key is pre-bucketed.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY orderkey) FROM orders",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        assertDistributedPlan("SELECT row_number() OVER (PARTITION BY orderkey) FROM orders",
                anyTree(
                        rowNumber(pattern -> pattern
                                        .partitionBy(ImmutableList.of("orderkey")),
                                project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        // Window partition key is not pre-bucketed.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY orderstatus) FROM orders",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("orderstatus"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus"))))))));

        assertDistributedPlan("SELECT row_number() OVER (PARTITION BY orderstatus) FROM orders",
                anyTree(
                        rowNumber(pattern -> pattern
                                        .partitionBy(ImmutableList.of("orderstatus")),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus"))))))));

        //Window to TopN RankingFunction
        for (RankingFunction rankingFunction : RankingFunction.values()) {
            assertDistributedPlan(format("SELECT orderkey FROM (SELECT orderkey, %s() OVER (PARTITION BY orderkey ORDER BY custkey) n FROM orders) WHERE n = 1", rankingFunction.getValue().getName()),
                    anyTree(
                            topNRankingNumber(pattern -> pattern
                                            .specification(
                                                    ImmutableList.of("orderkey"),
                                                    ImmutableList.of("custkey"),
                                                    ImmutableMap.of("custkey", ASC_NULLS_LAST)),
                                    project(tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey"))))));

            assertDistributedPlan(format("SELECT orderstatus FROM (SELECT orderstatus, %s() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n = 1", rankingFunction.getValue().getName()),
                    anyTree(
                            topNRankingNumber(pattern -> pattern
                                            .specification(
                                                    ImmutableList.of("orderstatus"),
                                                    ImmutableList.of("custkey"),
                                                    ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                            .partial(false),
                                    exchange(LOCAL, GATHER,
                                            exchange(REMOTE, REPARTITION,
                                                    topNRankingNumber(topNRowNumber -> topNRowNumber
                                                                    .specification(
                                                                            ImmutableList.of("orderstatus"),
                                                                            ImmutableList.of("custkey"),
                                                                            ImmutableMap.of("custkey", ASC_NULLS_LAST))
                                                                    .partial(true),
                                                            project(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "custkey", "custkey")))))))));
        }
    }

    @Test
    public void testWindowAfterJoin()
    {
        // Window partition key is a super set of join key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.orderstatus, o.orderkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("orderstatus", "orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        project(
                                                join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(PARTITIONED),
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "orderkey", "orderkey")))),
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE, REPARTITION,
                                                                        anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus")))))))))));

        // Window partition key is not a super set of join key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.orderkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(PARTITIONED),
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "orderkey", "orderkey")))),
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE, REPARTITION,
                                                                        anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus"))))))))))));

        // Test broadcast join
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .build();
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY o.custkey) FROM orders o JOIN lineitem l ON o.orderstatus = l.linestatus",
                broadcastJoin,
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                project(
                                                        join(INNER, ImmutableList.of(equiJoinClause("orderstatus", "linestatus")), Optional.empty(), Optional.of(REPLICATED),
                                                                anyTree(tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus", "custkey", "custkey"))),
                                                                exchange(LOCAL, GATHER,
                                                                        exchange(REMOTE, REPLICATE,
                                                                                anyTree(tableScan("lineitem", ImmutableMap.of("linestatus", "linestatus"))))))))))));
    }

    @Test
    public void testWindowAfterAggregation()
    {
        // Window partition key is a super set of group by key.
        assertDistributedPlan("SELECT rank() OVER (PARTITION BY custkey) FROM orders GROUP BY custkey",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                project(aggregation(singleGroupingSet("custkey"), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), FINAL,
                                        exchange(LOCAL, GATHER,
                                                project(exchange(REMOTE, REPARTITION,
                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey", "custkey")))))))))));

        // Window partition key is not a super set of group by key.
        assertDistributedPlan("SELECT rank() OVER (partition by custkey) FROM (SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY shippriority, custkey)",
                anyTree(
                        window(pattern -> pattern
                                        .specification(specification(ImmutableList.of("custkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("rank", Optional.empty(), ImmutableList.of())),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                project(aggregation(singleGroupingSet("shippriority", "custkey"), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), FINAL,
                                                        exchange(LOCAL, GATHER,
                                                                exchange(REMOTE, REPARTITION,
                                                                        anyTree(tableScan("orders", ImmutableMap.of("custkey", "custkey", "shippriority", "shippriority"))))))))))));
    }
}
