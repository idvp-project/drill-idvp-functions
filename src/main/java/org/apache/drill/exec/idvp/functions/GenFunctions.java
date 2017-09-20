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
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * @author Oleg Zinoviev
 * @since 20.09.2017
 **/
@SuppressWarnings("unused")
public class GenFunctions {
    private GenFunctions() {
    }

    @FunctionTemplate(
            name = "generate_uuid",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            isRandom = true
    )
    public static class GenerateUUID implements DrillSimpleFunc {

        @Output
        VarCharHolder out;
        @Inject
        DrillBuf buffer;

        public GenerateUUID() {
        }

        @Override
        public void setup() {

        }

        @Override
        public void eval() {
            String result = java.util.UUID.randomUUID().toString();
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
