/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.dsl.operator;

import com.antgroup.geaflow.dsl.sqlnode.SqlLetStatement;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlLetOperator extends SqlOperator {

    public static final SqlLetOperator INSTANCE = new SqlLetOperator();

    private SqlLetOperator() {
        super("Let", SqlKind.GQL_LET, 2, true,
            ReturnTypes.SCOPE, null, null);
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
        return new SqlLetStatement(pos, operands[0], (SqlIdentifier) operands[1], operands[2], false);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    @Override
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        call.unparse(writer, leftPrec, rightPrec);
    }
}
