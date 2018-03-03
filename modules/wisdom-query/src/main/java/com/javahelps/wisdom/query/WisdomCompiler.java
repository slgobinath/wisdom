package com.javahelps.wisdom.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.query.antlr.WisdomQLBaseVisitorImpl;
import com.javahelps.wisdom.query.antlr4.WisdomQLLexer;
import com.javahelps.wisdom.query.antlr4.WisdomQLParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class WisdomCompiler {

    public static WisdomApp parse(String source) {

        CodePointCharStream input = CharStreams.fromString(source);
        WisdomQLLexer lexer = new WisdomQLLexer(input);
        lexer.removeErrorListeners();

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        WisdomQLParser parser = new WisdomQLParser(tokens);
        ParseTree tree = parser.wisdom_app();

        WisdomQLBaseVisitorImpl eval = new WisdomQLBaseVisitorImpl();
        return (WisdomApp) eval.visit(tree);
    }
}
