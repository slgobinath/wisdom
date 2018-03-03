package com.javahelps.wisdom.query.tree;

import com.javahelps.wisdom.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class QueryNode {

    private String name;
    private final String input;
    private final String output;
    private final List<Statement> statements = new ArrayList<>();

    public QueryNode(String input, String output) {
        this.input = input;
        this.output = output;
    }

    public String getInput() {
        return input;
    }

    public String getOutput() {
        return output;
    }

    public String getName() {
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

    public void build(Query query) {
        query.from(this.input);
        for (Statement statement : this.statements) {
            statement.addTo(query);
        }
        query.insertInto(this.output);
    }
}
