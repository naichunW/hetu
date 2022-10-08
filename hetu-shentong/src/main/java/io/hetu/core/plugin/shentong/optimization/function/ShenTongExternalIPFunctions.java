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
package io.hetu.core.plugin.shentong.optimization.function;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.type.StandardTypes;

import java.util.Set;

public final class ShenTongExternalIPFunctions
{
    public static Set<ExternalFunctionInfo> getFunctionsInfo()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .add(SHENTONG_F_FROMIPV4_FUNCTION_INFO)
                .add(SHENTONG_F_TOIPV4_FUNCTION_INFO)
                .add(SHENTONG_F_TOIPV4INT_BIGINT_FUNCTION_INFO)
                .add(SHENTONG_F_TOIPV4INT_VARCHAR_FUNCTION_INFO)
                .add(SHENTONG_INET_NTOA_FUNCTION_INFO)
                .add(SHENTONG_INET_ATON_FUNCTION_INFO)
                .add(SHENTONG_INT4_TO_IPV4_FUNCTION_INFO)
                .add(SHENTONG_INT8_TO_IPV4_FUNCTION_INFO)
                .add(SHENTONG_IPV4_TO_INT4_FUNCTION_INFO)
                .add(SHENTONG_IPV4_TO_INT8_FUNCTION_INFO)
                .build();
    }

    private static final ExternalFunctionInfo SHENTONG_F_FROMIPV4_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("f_fromipv4")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT8 数")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_F_TOIPV4_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("f_toipv4")
                    .inputArgs(StandardTypes.BIGINT)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("当输入的一个 INT8 数值在范围 [0，4294967295] 时，可以转换成一个有效的 IPV4 地址，表示的 IPV4 地 址为(0.0.0.0)到(255.255.255.255);否则不在这个范围时，无法转换成一个有效的 IPV4 地址")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_F_TOIPV4INT_BIGINT_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("f_toipv4int")
                    .inputArgs(StandardTypes.BIGINT)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT8 数")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_F_TOIPV4INT_VARCHAR_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("f_toipv4int")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT8 数")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_INET_NTOA_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("inet_ntoa")
                    .inputArgs(StandardTypes.BIGINT)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将一个 INT8 数转换成 IPV4 地址")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_INET_ATON_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("inet_aton")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT8 数")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_INT4_TO_IPV4_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("int4_to_ipv4")
                    .inputArgs(StandardTypes.INTEGER)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将一个 INT4 数转换成 IPV4 地址")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_INT8_TO_IPV4_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("int8_to_ipv4")
                    .inputArgs(StandardTypes.BIGINT)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将一个 INT8 数转换成 IPV4 地址")
                    .build();


    private static final ExternalFunctionInfo SHENTONG_IPV4_TO_INT4_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("ipv4_to_int4")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.INTEGER)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT4 数")
                    .build();

    private static final ExternalFunctionInfo SHENTONG_IPV4_TO_INT8_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("ipv4_to_int8")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("将 IPV4 地址转换成一个 INT8 数")
                    .build();



    private ShenTongExternalIPFunctions()
    {
    }
}
