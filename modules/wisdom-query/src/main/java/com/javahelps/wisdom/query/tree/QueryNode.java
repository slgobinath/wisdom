package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class QueryNode {

    private final String input;
    private final List<Statement> statements = new ArrayList<>();
    private String name;

    public QueryNode(String input) {
        this.input = input;
    }

    public String getName() {
        if (name == null) {
            name = UUID.randomUUID().toString();
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);
    }

    public List<Statement> getStatements() {
        return statements;
    }

    public void build(WisdomApp app, Query query) {
        query.from(this.input);
        for (Statement statement : this.statements) {
            statement.addTo(app, query);
        }
    }
}
