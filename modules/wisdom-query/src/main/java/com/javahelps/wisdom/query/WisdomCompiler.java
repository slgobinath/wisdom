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

    public static WisdomApp parse(String source) {

        CodePointCharStream input = CharStreams.fromString(source);
        WisdomQLLexer lexer = new WisdomQLLexer(input);
        lexer.removeErrorListeners();

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        WisdomQLParser parser = new WisdomQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new WisdomErrorListener());
        ParseTree tree = parser.wisdom_app();

        WisdomQLBaseVisitorImpl eval = new WisdomQLBaseVisitorImpl();
        return (WisdomApp) eval.visit(tree);
    }

    public static WisdomApp parse(Path source) throws IOException {

        CharStream input = CharStreams.fromFileName(source.toAbsolutePath().toString(), Charset.defaultCharset());
        WisdomQLLexer lexer = new WisdomQLLexer(input);
        lexer.removeErrorListeners();

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        WisdomQLParser parser = new WisdomQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new WisdomErrorListener());
        ParseTree tree = parser.wisdom_app();

        WisdomQLBaseVisitorImpl eval = new WisdomQLBaseVisitorImpl();
        return (WisdomApp) eval.visit(tree);
    }
}
