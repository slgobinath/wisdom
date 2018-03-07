package com.javahelps.wisdom.query.antlr;

import org.antlr.v4.runtime.ParserRuleContext;

public class WisdomParserException extends RuntimeException {
    public WisdomParserException(ParserRuleContext ctx, String msg) {
        super(String.format("[%d:%d] Syntax error in WisdomQL, near '%s': %s",
                ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(),
                ctx.getText(), msg));
    }

    public WisdomParserException(String msg, int line, int position) {
        super(String.format("[%d:%d] Syntax error in WisdomQL: %s",
                line, position, msg));
    }
}
