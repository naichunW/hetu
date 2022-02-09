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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinNode.DistributionType;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.planner.RowExpressionEqualityInference;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static io.prestosql.SystemSessionProperties.getJoinDistributionType;
import static io.prestosql.SystemSessionProperties.getJoinReorderingStrategy;
import static io.prestosql.SystemSessionProperties.getMaxReorderedJoins;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static io.prestosql.sql.planner.RowExpressionEqualityInference.createEqualityInference;
import static io.prestosql.sql.planner.iterative.rule.DetermineJoinDistributionType.canReplicate;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static io.prestosql.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private final Pattern<JoinNode> joinNodePattern;

    private final CostComparator costComparator;

    private final DeterminismEvaluator determinismEvaluator;

    private final Metadata metadata;

    private final LogicalRowExpressions logicalRowExpressions;

    public ReorderJoins(CostComparator costComparator, Metadata metadata)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
        this.joinNodePattern = join().matching(
                joinNode -> !joinNode.getDistributionType().isPresent()
                        && joinNode.getType() == INNER
                        && determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT)));
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                                                                new FunctionResolution(metadata.getFunctionAndTypeManager()),
                                                                metadata.getFunctionAndTypeManager());
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return joinNodePattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == AUTOMATIC && SystemSessionProperties.getSkipReorderingThreshold(session) > 0;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode,
                context.getLookup(),
                getMaxReorderedJoins(context.getSession()),
                metadata,
                context.getSymbolAllocator().getTypes(),
                logicalRowExpressions);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                context,
                metadata,
                logicalRowExpressions);
        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }
        return Result.ofPlanNode(result.getPlanNode().get());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final RowExpression allFilter;
        private final RowExpressionEqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;
        private final Metadata metadata;
        private final LogicalRowExpressions logicalRowExpressions;

        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, RowExpression filter, Context context, Metadata metadata, LogicalRowExpressions logicalRowExpressions)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.allFilterInference = createEqualityInference(metadata, filter);
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");
            this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent((planNode) -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }

        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Set<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size())
                    .collect(toImmutableSet());
        }

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            List<PlanNode> sourceList = ImmutableList.copyOf(sources);
            LinkedHashSet<PlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(toCollection(LinkedHashSet::new));
            LinkedHashSet<PlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputSymbols);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<Symbol> outputSymbols)
        {
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());

            List<RowExpression> joinPredicates = getJoinPredicates(getVariables(leftSymbols, context.getSymbolAllocator().getTypes()),
                    getVariables(rightSymbols, context.getSymbolAllocator().getTypes()));
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(rowExpression -> isJoinEqualityCondition(rowExpression))
                    .map(predicate -> toEquiJoinClause((CallExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<RowExpression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(SymbolsExtractor.extractUnique(joinPredicates, null))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinSymbols.stream()
                            .filter(leftSymbols::contains)
                            .collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            // sort output symbols so that the left input symbols are first
            List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    sortedOutputSymbols,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(logicalRowExpressions.and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of()));
        }

        private Set<VariableReferenceExpression> getVariables(Set<Symbol> symbols, TypeProvider types)
        {
            Set<VariableReferenceExpression> variables = new HashSet<>();
            for (Symbol symbol : symbols) {
                variables.add(new VariableReferenceExpression(symbol.getName(), types.get(symbol)));
            }
            return variables;
        }

        private List<RowExpression> getJoinPredicates(Set<VariableReferenceExpression> leftVariables, Set<VariableReferenceExpression> rightVariables)
        {
            ImmutableList.Builder<RowExpression> joinPredicatesBuilder = ImmutableList.builder();

            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right symbols, we add them to the list of joinPredicates
            RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata);
            StreamSupport.stream(builder.nonInferrableConjuncts(allFilter).spliterator(), false)
                    .map(conjunct -> allFilterInference.rewriteExpression(conjunct, variable -> leftVariables.contains(variable) || rightVariables.contains(variable)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftVariables::contains) == null)
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightVariables::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<RowExpression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(
                    variable -> leftVariables.contains(variable) || rightVariables.contains(variable)).getScopeEqualities();
            RowExpressionEqualityInference joinInference = createEqualityInference(metadata, joinEqualities.toArray(new RowExpression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftVariables)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<Symbol> outputSymbols)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
                RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata);
                StreamSupport.stream(builder.nonInferrableConjuncts(allFilter).spliterator(), false)
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputSymbols::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                RowExpression filter = logicalRowExpressions.combineConjuncts(predicates.build());
                if (!TRUE_CONSTANT.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, outputSymbols);
        }

        private boolean isJoinEqualityCondition(RowExpression expression)
        {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
                return functionMetadata.getOperatorType().isPresent()
                        && functionMetadata.getOperatorType().get().equals(OperatorType.EQUAL)
                        && call.getArguments().size() == 2
                        && call.getArguments().get(0) instanceof VariableReferenceExpression
                        && call.getArguments().get(1) instanceof VariableReferenceExpression;
            }
            return false;
        }

        private static EquiJoinClause toEquiJoinClause(CallExpression equality, Set<Symbol> leftSymbols)
        {
            checkArgument(equality.getArguments().size() == 2, "Unexpected number of arguments in binary operator equals");
            VariableReferenceExpression leftVariable = (VariableReferenceExpression) equality.getArguments().get(0);
            VariableReferenceExpression rightVariable = (VariableReferenceExpression) equality.getArguments().get(1);
            Symbol leftSymbol = new Symbol(leftVariable.getName());
            EquiJoinClause equiJoinClause = new EquiJoinClause(leftSymbol, new Symbol(rightVariable.getName()));
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            List<JoinEnumerationResult> possibleJoinNodes = getPossibleJoinNodes(joinNode, getJoinDistributionType(session));
            verify(!possibleJoinNodes.isEmpty(), "possibleJoinNodes is empty");
            if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, JoinDistributionType distributionType)
        {
            checkArgument(joinNode.getType() == INNER, "unexpected join node type: %s", joinNode.getType());

            if (joinNode.isCrossJoin()) {
                return getPossibleJoinNodes(joinNode, REPLICATED);
            }

            switch (distributionType) {
                case PARTITIONED:
                    return getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST:
                    return getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC:
                    ImmutableList.Builder<JoinEnumerationResult> result = ImmutableList.builder();
                    result.addAll(getPossibleJoinNodes(joinNode, PARTITIONED));
                    result.addAll(getPossibleJoinNodes(joinNode, REPLICATED, node -> canReplicate(node, context)));

                    return result.build();
                default:
                    throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
            }
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return getPossibleJoinNodes(joinNode, distributionType, (node) -> true);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType, Predicate<JoinNode> isAllowed)
        {
            List<JoinNode> nodes = ImmutableList.of(
                    joinNode.withDistributionType(distributionType),
                    joinNode.flipChildren().withDistributionType(distributionType));
            return nodes.stream().filter(isAllowed).map(this::createJoinEnumerationResult).collect(toImmutableList());
        }

        private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCost(planNode));
        }
    }

    /**
     * This class represents a set of inner joins that can be executed in any order.
     */
    @VisibleForTesting
    static class MultiJoinNode
    {
        // Use a linked hash set to ensure optimizer is deterministic
        private final LinkedHashSet<PlanNode> sources;
        private final RowExpression filter;
        private final List<Symbol> outputSymbols;
        private final LogicalRowExpressions logicalRowExpressions;

        public MultiJoinNode(LinkedHashSet<PlanNode> sources, RowExpression filter, List<Symbol> outputSymbols, LogicalRowExpressions logicalRowExpressions)
        {
            requireNonNull(sources, "sources is null");
            checkArgument(sources.size() > 1, "sources size is <= 1");
            requireNonNull(filter, "filter is null");
            requireNonNull(outputSymbols, "outputSymbols is null");
            requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");

            this.sources = sources;
            this.filter = filter;
            this.outputSymbols = ImmutableList.copyOf(outputSymbols);
            this.logicalRowExpressions = logicalRowExpressions;

            List<Symbol> inputSymbols = sources.stream().flatMap(source -> source.getOutputSymbols().stream()).collect(toImmutableList());
            checkArgument(inputSymbols.containsAll(outputSymbols), "inputs do not contain all output symbols");
        }

        public RowExpression getFilter()
        {
            return filter;
        }

        public LinkedHashSet<PlanNode> getSources()
        {
            return sources;
        }

        public List<Symbol> getOutputSymbols()
        {
            return outputSymbols;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sources, ImmutableSet.copyOf(LogicalRowExpressions.extractConjuncts(filter)), outputSymbols);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(LogicalRowExpressions.extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(LogicalRowExpressions.extractConjuncts(other.filter)))
                    && this.outputSymbols.equals(other.outputSymbols);
        }

        static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit, Metadata metadata, TypeProvider types, LogicalRowExpressions logicalRowExpressions)
        {
            // the number of sources is the number of joins + 1
            return new JoinNodeFlattener(joinNode,
                    lookup,
                    joinLimit + 1,
                    new RowExpressionDeterminismEvaluator(metadata),
                    types,
                    logicalRowExpressions).toMultiJoinNode();
        }

        private static class JoinNodeFlattener
        {
            private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
            private final List<RowExpression> filters = new ArrayList<>();
            private final List<Symbol> outputSymbols;
            private final Lookup lookup;
            private final DeterminismEvaluator determinismEvaluator;
            private final TypeProvider types;
            private final LogicalRowExpressions logicalRowExpressions;

            JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit, DeterminismEvaluator determinismEvaluator, TypeProvider types, LogicalRowExpressions logicalRowExpressions)
            {
                requireNonNull(node, "node is null");
                checkState(node.getType() == INNER, "join type must be INNER");
                this.outputSymbols = node.getOutputSymbols();
                this.lookup = requireNonNull(lookup, "lookup is null");
                this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
                this.types = requireNonNull(types, "types is null");
                this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
                flattenNode(node, sourceLimit);
            }

            private void flattenNode(PlanNode node, int limit)
            {
                PlanNode resolved = lookup.resolve(node);

                // (limit - 2) because you need to account for adding left and right side
                if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                    sources.add(node);
                    return;
                }

                JoinNode joinNode = (JoinNode) resolved;
                if (joinNode.getType() != INNER
                        || !determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT))
                        || joinNode.getDistributionType().isPresent()) {
                    sources.add(node);
                    return;
                }

                // we set the left limit to limit - 1 to account for the node on the right
                flattenNode(joinNode.getLeft(), limit - 1);
                flattenNode(joinNode.getRight(), limit);
                joinNode.getCriteria().stream()
                        .map(criteria -> toRowExpression(criteria, types))
                        .forEach(filters::add);
                joinNode.getFilter().ifPresent(filters::add);
            }

            MultiJoinNode toMultiJoinNode()
            {
                return new MultiJoinNode(sources, logicalRowExpressions.and(filters), outputSymbols, logicalRowExpressions);
            }
        }

        static class Builder
        {
            private List<PlanNode> sources;
            private RowExpression filter;
            private List<Symbol> outputSymbols;
            private LogicalRowExpressions logicalRowExpressions;

            public Builder setSources(PlanNode... sources)
            {
                this.sources = ImmutableList.copyOf(sources);
                return this;
            }

            public Builder setFilter(RowExpression filter)
            {
                this.filter = filter;
                return this;
            }

            public Builder setOutputSymbols(Symbol... outputSymbols)
            {
                this.outputSymbols = ImmutableList.copyOf(outputSymbols);
                return this;
            }

            public Builder setLogicalRowExpressions(LogicalRowExpressions logicalRowExpressions)
            {
                this.logicalRowExpressions = logicalRowExpressions;
                return this;
            }

            public MultiJoinNode build()
            {
                return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputSymbols, logicalRowExpressions);
            }
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        public static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        public static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<PlanNode> planNode;
        private final PlanCostEstimate cost;

        private JoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && !planNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(planNode, cost);
        }
    }
}
