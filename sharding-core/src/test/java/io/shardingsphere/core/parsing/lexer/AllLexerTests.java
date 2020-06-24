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

package io.shardingsphere.core.parsing.lexer;

import io.shardingsphere.core.parsing.lexer.analyzer.CharTypeTest;
import io.shardingsphere.core.parsing.lexer.analyzer.TokenizerTest;
import io.shardingsphere.core.parsing.lexer.dialect.mysql.MySQLLexerTest;
import io.shardingsphere.core.parsing.lexer.dialect.oracle.OracleLexerTest;
import io.shardingsphere.core.parsing.lexer.dialect.postgresql.PostgreSQLLexerTest;
import io.shardingsphere.core.parsing.lexer.dialect.sqlserver.SQLServerLexerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        CharTypeTest.class,
        TokenizerTest.class,
        LexerTest.class,
        MySQLLexerTest.class,
        OracleLexerTest.class,
        SQLServerLexerTest.class,
        PostgreSQLLexerTest.class
    })
public final class AllLexerTests {
}
