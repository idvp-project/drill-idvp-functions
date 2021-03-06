/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.idvp.functions;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 29.08.2017
 **/
@SuppressWarnings({"unused", "Duplicates", "deprecation"})
public class JoinFunctions {
    private JoinFunctions() {
    }

    @FunctionTemplate(names = {"str_join", "strjoin", "megaStringerator4000"},
            scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
            nulls = FunctionTemplate.NullHandling.INTERNAL)
    public static class NullableStringJoin implements DrillAggFunc {
        @Param
        NullableVarCharHolder inParam;
        @Param
        VarCharHolder joinCharParam;

        @Workspace
        ObjectHolder holder;

        @Workspace
        ObjectHolder empty;

        @Output
        NullableVarCharHolder out;

        @Inject
        DrillBuf buffer;


        @Override
        public void setup() {
            holder = new ObjectHolder();
            holder.obj = new StringBuilder();
            empty = new ObjectHolder();
            empty.obj = Boolean.TRUE;
        }

        @Override
        public void add() {
            String item = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(inParam);
            if (item != null) {
                boolean first = Boolean.TRUE.equals(empty.obj);
                StringBuilder sb = (StringBuilder) holder.obj;

                String delimiter = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(joinCharParam);
                if (delimiter == null) {
                    delimiter = "";
                }

                if (!first) {
                    sb.append(delimiter);
                }

                sb.append(item);
                empty.obj = Boolean.FALSE;
            }
        }

        @Override
        public void output() {
            if (Boolean.TRUE.equals(empty.obj)) {
                out.isSet = 0;
            } else {
                StringBuilder sb = (StringBuilder) holder.obj;
                String result = sb.toString();

                out.isSet = 1;
                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                out.start = 0;
                out.end = bytes.length;
                out.buffer.setBytes(0, bytes);
            }
        }

        @Override
        public void reset() {
            holder = new ObjectHolder();
            holder.obj = new StringBuilder();
            empty = new ObjectHolder();
            empty.obj = Boolean.TRUE;
        }
    }

    @FunctionTemplate(names = {"str_join", "strjoin", "megaStringerator4000"},
            scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
            nulls = FunctionTemplate.NullHandling.INTERNAL)
    public static class StringJoin implements DrillAggFunc {
        @Param
        VarCharHolder inParam;
        @Param
        VarCharHolder joinCharParam;

        @Workspace
        ObjectHolder holder;

        @Workspace
        ObjectHolder empty;

        @Output
        NullableVarCharHolder out;

        @Inject
        DrillBuf buffer;


        @Override
        public void setup() {
            holder = new ObjectHolder();
            holder.obj = new StringBuilder();
            empty = new ObjectHolder();
            empty.obj = Boolean.TRUE;
        }

        @Override
        public void add() {
            String item = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(inParam);
            if (item != null) {
                boolean first = Boolean.TRUE.equals(empty.obj);
                StringBuilder sb = (StringBuilder) holder.obj;

                String delimiter = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(joinCharParam);
                if (delimiter == null) {
                    delimiter = "";
                }

                if (!first) {
                    sb.append(delimiter);
                }

                sb.append(item);
                empty.obj = Boolean.FALSE;
            }
        }

        @Override
        public void output() {
            if (Boolean.TRUE.equals(empty.obj)) {
                out.isSet = 0;
            } else {
                StringBuilder sb = (StringBuilder) holder.obj;
                String result = sb.toString();

                out.isSet = 1;
                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                out.start = 0;
                out.end = bytes.length;
                out.buffer.setBytes(0, bytes);
            }
        }

        @Override
        public void reset() {
            holder = new ObjectHolder();
            holder.obj = new StringBuilder();
            empty = new ObjectHolder();
            empty.obj = Boolean.TRUE;
        }
    }


//    @FunctionTemplate(name = "join_list_to_string",
//            scope = FunctionTemplate.FunctionScope.SIMPLE,
//            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
//    public static class StringListJoin implements DrillSimpleFunc {
//        @Param
//        RepeatedVarCharHolder inParam;
//        @Param
//        VarCharHolder joinCharParam;
//
//        @Output
//        VarCharHolder out;
//
//        @Inject
//        DrillBuf buffer;
//
//
//        @Override
//        public void setup() {
//        }
//
//        @Override
//        public void eval() {
//            StringBuilder sb = new StringBuilder();
//            String delimiter = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(joinCharParam);
//
//            for (int i = inParam.start; i < inParam.end; i++) {
//                byte[] bytes = inParam.vector.getAccessor().get(i);
//                String string = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
//
//                sb = sb.append(string);
//                if (i != inParam.end - 1) {
//                    sb.append(delimiter);
//                }
//            }
//
//            String result = sb.toString();
//            byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
//            out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
//            out.start = 0;
//            out.end = bytes.length;
//        }
//
//    }

}
