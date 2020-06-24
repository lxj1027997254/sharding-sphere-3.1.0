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

package io.shardingsphere.core.parsing.parser.token;

import io.shardingsphere.core.util.SQLUtil;
import lombok.Getter;
import lombok.ToString;

/**
 * Table token.
 *
 * @author zhangliang
 */
@Getter
@ToString
public final class TableToken extends SQLToken {
    
    private final int skippedSchemaNameLength;
    
    private final String originalLiterals;
    
    public TableToken(final int beginPosition, final int skippedSchemaNameLength, final String originalLiterals) {
        super(beginPosition);
        this.skippedSchemaNameLength = skippedSchemaNameLength;
        this.originalLiterals = originalLiterals;
    }
    
    /**
     * Get table name.
     * 
     * @return table name
     */
    public String getTableName() {
        return SQLUtil.getExactlyValue(originalLiterals);
    }
}
