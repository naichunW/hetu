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

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static io.hetu.core.plugin.shentong.ShenTongConstants.CONNECTOR_NAME;

@ConnectorConfig(connectorLabel = "ShenTong: Query data in ShenTong MPP",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "",
        configLink = "")
public class ShenTongSqlPlugin
        extends JdbcPlugin
{
    public ShenTongSqlPlugin()
    {
        super(CONNECTOR_NAME, new ShenTongClientModule());
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = ShenTongSqlPlugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(BaseJdbcConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(ShenTongSqlConfig.class.getDeclaredMethods()));
        return ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
    }
}
