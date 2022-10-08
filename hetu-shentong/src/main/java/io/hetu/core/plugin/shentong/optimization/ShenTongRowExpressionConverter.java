/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.shentong.optimization;

import io.hetu.core.plugin.shentong.ShenTongConstants;
import io.hetu.core.plugin.shentong.optimization.function.ShenTongApplyRemoteFunctionPushDown;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.*;

import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.isDefaultFunction;

public class ShenTongRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private final ShenTongApplyRemoteFunctionPushDown shenTongApplyRemoteFunctionPushDown;

    public ShenTongRowExpressionConverter(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, BaseJdbcConfig baseJdbcConfig)
    {
        super(functionManager, functionResolution, rowExpressionService, determinismEvaluator);
        this.shenTongApplyRemoteFunctionPushDown = new ShenTongApplyRemoteFunctionPushDown(baseJdbcConfig, ShenTongConstants.CONNECTOR_NAME);
    }

    @Override
    public String visitCall(CallExpression call, JdbcConverterContext context)
    {
        // remote udf verify
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (!isDefaultFunction(call)) {
            Optional<String> result = shenTongApplyRemoteFunctionPushDown.rewriteRemoteFunction(call, this, context);
            if (result.isPresent()) {
                return result.get();
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("ShenTong connector does not support remote function: %s.%s", call.getDisplayName(), call.getFunctionHandle().getFunctionNamespace()));
        }

        return super.visitCall(call, context);
    }
    
}
