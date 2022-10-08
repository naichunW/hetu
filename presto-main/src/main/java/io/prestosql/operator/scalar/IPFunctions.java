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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;


/**
 * TODO 增加ip转换函数
 */

public final class IPFunctions
{
    private IPFunctions() {}


    @Description("ipv4 num to string")
    @ScalarFunction("ipv4numtostring")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static String length(@SqlType("bigint") Slice slice)
    {
        return "";
    }

}
