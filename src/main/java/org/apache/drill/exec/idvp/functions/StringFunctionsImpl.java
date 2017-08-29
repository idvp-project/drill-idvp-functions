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
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import java.nio.charset.StandardCharsets;

/**
 * @author Oleg Zinoviev
 * @since 29.08.2017
 **/
@SuppressWarnings("WeakerAccess")
public class StringFunctionsImpl {

    public static void toUpper(VarCharHolder input, VarCharHolder out, DrillBuf buffer) {
        String string = StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
        string = string.toUpperCase();

        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);

        out.buffer = buffer.reallocIfNeeded(bytes.length);
        out.start = 0;
        out.end = bytes.length;

        out.buffer.writeBytes(bytes);
    }

    public static void toLower(VarCharHolder input, VarCharHolder out, DrillBuf buffer) {
        String string = StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
        string = string.toLowerCase();

        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        out.buffer = buffer.reallocIfNeeded(bytes.length);
        out.start = 0;
        out.end = bytes.length;

        out.buffer.writeBytes(bytes);
    }
}
