/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.idvp.functions;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 29.08.2017
 **/
@SuppressWarnings("unused")
public class StringFunctions {
    private StringFunctions() {
    }

    @FunctionTemplate(
            name = "to_upper",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class UpperCase implements DrillSimpleFunc {

        @Param
        VarCharHolder input;
        @Output
        VarCharHolder out;
        @Inject
        DrillBuf buffer;

        public UpperCase() {
        }

        @Override
        public void setup() {

        }

        @Override
        public void eval() {
            org.apache.drill.exec.idvp.functions.StringFunctionsImpl.toUpper(input, out, buffer);
        }
    }

    @FunctionTemplate(
            name = "to_lower",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class LowerCase implements DrillSimpleFunc {
        @Param
        VarCharHolder input;
        @Output
        VarCharHolder out;
        @Inject
        DrillBuf buffer;

        public LowerCase() {
        }

        public void setup() {
        }

        public void eval() {
            org.apache.drill.exec.idvp.functions.StringFunctionsImpl.toLower(input, out, buffer);
        }
    }

}
