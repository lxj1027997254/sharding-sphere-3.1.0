/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
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
 * </p>
 */

package io.shardingsphere.shardingjdbc.orchestration.api.yaml.masterslave;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.shardingsphere.shardingjdbc.orchestration.api.yaml.AbstractYamlDataSourceTest;
import io.shardingsphere.shardingjdbc.orchestration.api.yaml.YamlOrchestrationMasterSlaveDataSourceFactory;
import io.shardingsphere.shardingjdbc.orchestration.internal.datasource.OrchestrationMasterSlaveDataSource;
import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@RequiredArgsConstructor
public class YamlOrchestrationMasterSlaveIntegrateTest extends AbstractYamlDataSourceTest {
    
    private final String filePath;
    
    private final boolean hasDataSource;
    
    @Parameterized.Parameters(name = "{index}:{0}-{1}")
    public static Collection init() {
        return Arrays.asList(new Object[][]{
                {"/yaml/integrate/ms/configWithMasterSlaveDataSourceWithoutProps.yaml", true},
                {"/yaml/integrate/ms/configWithMasterSlaveDataSourceWithoutProps.yaml", false},
                {"/yaml/integrate/ms/configWithMasterSlaveDataSourceWithProps.yaml", true},
                {"/yaml/integrate/ms/configWithMasterSlaveDataSourceWithProps.yaml", false},
        });
    }
    
    @Test
    public void assertWithDataSource() throws SQLException, URISyntaxException, IOException {
        File yamlFile = new File(YamlOrchestrationMasterSlaveIntegrateTest.class.getResource(filePath).toURI());
        DataSource dataSource;
        if (hasDataSource) {
            dataSource = YamlOrchestrationMasterSlaveDataSourceFactory.createDataSource(yamlFile);
        } else {
            dataSource = YamlOrchestrationMasterSlaveDataSourceFactory.createDataSource(Maps.asMap(Sets.newHashSet("db_master", "db_slave_0", "db_slave_1"), new Function<String, DataSource>() {
                @Override
                public DataSource apply(final String key) {
                    return AbstractYamlDataSourceTest.createDataSource(key);
                }
            }), yamlFile);
        }
        try (Connection conn = dataSource.getConnection();
             Statement stm = conn.createStatement()) {
            stm.executeQuery("SELECT * FROM t_order");
            stm.executeQuery("SELECT * FROM t_order_item");
            stm.executeQuery("SELECT * FROM t_config");
        }
        ((OrchestrationMasterSlaveDataSource) dataSource).close();
    }
}
