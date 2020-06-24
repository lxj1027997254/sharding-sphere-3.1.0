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

package io.shardingsphere.core.parsing.parser.dialect.mysql.clause.facade;

import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.parser.clause.InsertColumnsClauseParser;
import io.shardingsphere.core.parsing.parser.clause.facade.AbstractInsertClauseParserFacade;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLInsertDuplicateKeyUpdateClauseParser;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLInsertIntoClauseParser;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLInsertSetClauseParser;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLInsertValuesClauseParser;
import io.shardingsphere.core.rule.ShardingRule;

/**
 * Insert clause parser facade for MySQL.
 *
 * @author zhangliang
 */
public final class MySQLInsertClauseParserFacade extends AbstractInsertClauseParserFacade {
    
    public MySQLInsertClauseParserFacade(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        super(new MySQLInsertIntoClauseParser(shardingRule, lexerEngine), new InsertColumnsClauseParser(shardingRule, lexerEngine), 
                new MySQLInsertValuesClauseParser(shardingRule, lexerEngine), new MySQLInsertSetClauseParser(shardingRule, lexerEngine),
                new MySQLInsertDuplicateKeyUpdateClauseParser(shardingRule, lexerEngine));
    }
}
