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
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 29.08.2017
 **/
@SuppressWarnings({"unused", "Duplicates"})
public class JoinFunctions {
    private JoinFunctions() {
    }

    @FunctionTemplate(names = { "str_join", "strjoin", "megaStringerator4000" },
            scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
            nulls = FunctionTemplate.NullHandling.INTERNAL)
    public static class NullableStringJoin implements DrillAggFunc {
        @Param
        NullableVarCharHolder inParam;
        @Param
        VarCharHolder joinCharParam;

        @Workspace
        NullableVarCharHolder holder;

        @Output
        NullableVarCharHolder out;

        @Inject
        DrillBuf buffer;


        @Override
        public void setup() {
            holder = new NullableVarCharHolder();
            holder.isSet = 0;
        }

        @Override
        public void add() {
            String item = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(inParam);
            if (item != null) {
                boolean first = holder.isSet == 0;

                String previous = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(holder);
                if (previous == null) {
                    previous = "";
                }

                String result;
                if (first) {
                    result = item;
                } else {
                    String delimiter = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(joinCharParam);
                    if (delimiter == null) {
                        delimiter = "";
                    }
                    result = previous + delimiter + item;
                }

                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                holder.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                holder.isSet = 1;
                holder.start = 0;
                holder.end = bytes.length;

                for (int id = 0; id < bytes.length; ++id) {
                    byte currentByte = bytes[id];
                    holder.buffer.setByte(id, currentByte);
                }
            }
        }

        @Override
        public void output() {
            if (holder.isSet == 0) {
                out.isSet = 0;
            } else {
                String result = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(holder);
                if (result == null) {
                    result = "";
                }

                out.isSet = 1;
                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                out.start = 0;
                out.end = bytes.length;
            }
        }

        @Override
        public void reset() {
            holder.isSet = 0;
            holder.start = 0;
            holder.end = 0;
            holder.buffer = null;
        }
    }

    @FunctionTemplate(names = { "str_join", "strjoin", "megaStringerator4000" },
            scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
            nulls = FunctionTemplate.NullHandling.INTERNAL)
    public static class StringJoin implements DrillAggFunc {
        @Param
        VarCharHolder inParam;
        @Param
        VarCharHolder joinCharParam;

        @Workspace
        NullableVarCharHolder holder;

        @Output
        NullableVarCharHolder out;

        @Inject
        DrillBuf buffer;


        @Override
        public void setup() {
            holder = new NullableVarCharHolder();
            holder.isSet = 0;
        }

        @Override
        public void add() {
            String item = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(inParam);
            if (item != null) {
                boolean first = holder.isSet == 0;

                String previous = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(holder);
                if (previous == null) {
                    previous = "";
                }

                String result;
                if (first) {
                    result = item;
                } else {
                    String delimiter = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(joinCharParam);
                    if (delimiter == null) {
                        delimiter = "";
                    }
                    result = previous + delimiter + item;
                }

                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                holder.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                holder.isSet = 1;
                holder.start = 0;
                holder.end = bytes.length;

                for (int id = 0; id < bytes.length; ++id) {
                    byte currentByte = bytes[id];
                    holder.buffer.setByte(id, currentByte);
                }
            }
        }

        @Override
        public void output() {
            if (holder.isSet == 0) {
                out.isSet = 0;
            } else {
                String result = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(holder);
                if (result == null) {
                    result = "";
                }

                out.isSet = 1;
                byte[] bytes = result.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.buffer = buffer = buffer.reallocIfNeeded(bytes.length);
                out.start = 0;
                out.end = bytes.length;
            }
        }

        @Override
        public void reset() {
            holder.isSet = 0;
            holder.start = 0;
            holder.end = 0;
            holder.buffer = null;
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
