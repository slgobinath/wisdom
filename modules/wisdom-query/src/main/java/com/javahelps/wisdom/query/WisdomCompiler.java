/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.query.antlr.WisdomErrorListener;
import com.javahelps.wisdom.query.antlr.WisdomQLBaseVisitorImpl;
import com.javahelps.wisdom.query.antlr4.WisdomQLLexer;
import com.javahelps.wisdom.query.antlr4.WisdomQLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

public class WisdomCompiler {

    private WisdomCompiler() {

    }

    public static WisdomApp parse(CharStream source) {

        WisdomQLLexer lexer = new WisdomQLLexer(source);
        lexer.removeErrorListeners();

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        WisdomQLParser parser = new WisdomQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new WisdomErrorListener());
        ParseTree tree = parser.wisdom_app();

        WisdomQLBaseVisitorImpl eval = new WisdomQLBaseVisitorImpl();
        return (WisdomApp) eval.visit(tree);
    }

    public static WisdomApp parse(String source) {

        CodePointCharStream input = CharStreams.fromString(source);
        return WisdomCompiler.parse(input);
    }

    public static WisdomApp parse(Path source) throws IOException {

        CharStream input = CharStreams.fromFileName(source.toAbsolutePath().toString(), Charset.defaultCharset());
        return WisdomCompiler.parse(input);
    }
}
