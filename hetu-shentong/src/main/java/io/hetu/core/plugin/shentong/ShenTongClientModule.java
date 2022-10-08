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
package io.hetu.core.plugin.shentong;

import com.google.inject.*;
import io.hetu.core.plugin.shentong.optimization.function.ShenTongExternalFunctionHub;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import com.oscar.Driver;
import io.prestosql.spi.function.ExternalFunctionHub;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ShenTongClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).to(ShenTongSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(ExternalFunctionHub.class).to(ShenTongExternalFunctionHub.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ShenTongSqlConfig.class);
    }

    @Provides
    @Singleton
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config)
    {
        return new DriverConnectionFactory(new Driver(), config);
    }
}
