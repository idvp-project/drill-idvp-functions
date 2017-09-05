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
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            returnType = FunctionTemplate.ReturnType.SAME_IN_OUT_LENGTH
    )
    public static class UpperCase implements DrillSimpleFunc {

        @Param
        VarCharHolder string1Param;
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
            String result = org.apache.drill.exec.idvp.functions.StringFunctionsImpl.toUpper(string1Param);
            byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
            out.start = 0;
            out.end = bytes.length;

            for(int id = 0; id < bytes.length; ++id) {
                byte currentByte = bytes[id];
                out.buffer.setByte(id, currentByte);
            }
        }
    }

    @FunctionTemplate(
            name = "to_lower",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
            returnType = FunctionTemplate.ReturnType.SAME_IN_OUT_LENGTH
    )
    public static class LowerCase implements DrillSimpleFunc {
        @Param
        VarCharHolder string1Param;
        @Output
        VarCharHolder out;
        @Inject
        DrillBuf buffer;

        public LowerCase() {
        }

        public void setup() {
        }

        public void eval() {
            String result = org.apache.drill.exec.idvp.functions.StringFunctionsImpl.toLower(string1Param);
            byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
            out.start = 0;
            out.end = bytes.length;

            for(int id = 0; id < bytes.length; ++id) {
                byte currentByte = bytes[id];
                out.buffer.setByte(id, currentByte);
            }
        }
    }

}
