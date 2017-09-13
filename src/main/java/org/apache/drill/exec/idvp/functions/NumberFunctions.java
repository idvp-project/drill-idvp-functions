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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/**
 * @author Oleg Zinoviev
 * @since 13.09.2017
 **/
@SuppressWarnings({"unused","Duplicates"})
public class NumberFunctions {
    private NumberFunctions() {
    }

    @FunctionTemplate(name = "to_number",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class ToNumber2 implements DrillSimpleFunc {
        @Param
        VarCharHolder left;
        @Param
        VarCharHolder right;
        @Param
        VarCharHolder decimalSeparator;

        @Workspace
        java.text.DecimalFormat inputFormat;
        @Workspace
        int decimalDigits;

        @Output
        Float8Holder out;

        public void setup() {
            byte[] buf = new byte[right.end - right.start];
            right.buffer.getBytes(right.start, buf, 0, right.end - right.start);

            java.text.DecimalFormatSymbols symbols = java.text.DecimalFormatSymbols.getInstance(java.util.Locale.getDefault(java.util.Locale.Category.FORMAT));

            String decimalSeparatorStr = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(decimalSeparator);
            if (decimalSeparatorStr != null && !decimalSeparatorStr.isEmpty()) {
                symbols.setDecimalSeparator(decimalSeparatorStr.charAt(0));
            }

            inputFormat = new java.text.DecimalFormat(new String(buf), symbols);

            decimalDigits = inputFormat.getMaximumFractionDigits();
        }

        public void eval() {
            byte[] buf1 = new byte[left.end - left.start];
            left.buffer.getBytes(left.start, buf1, 0, left.end - left.start);
            String input = new String(buf1);
            try {
                out.value = inputFormat.parse(input).doubleValue();
            }  catch(java.text.ParseException e) {
                throw new UnsupportedOperationException("Cannot parse input: " + input + " with pattern : " + inputFormat.toPattern());
            }

            // Round the value
            java.math.BigDecimal roundedValue = new java.math.BigDecimal(out.value);
            out.value = (roundedValue.setScale(decimalDigits, java.math.BigDecimal.ROUND_HALF_UP)).doubleValue();
        }
    }

    @FunctionTemplate(name = "to_number",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class ToNumber3 implements DrillSimpleFunc {
        @Param
        VarCharHolder left;
        @Param
        VarCharHolder right;
        @Param
        VarCharHolder decimalSeparator;
        @Param
        VarCharHolder groupSeparator;

        @Workspace
        java.text.DecimalFormat inputFormat;
        @Workspace
        int decimalDigits;

        @Output
        Float8Holder out;

        public void setup() {
            byte[] buf = new byte[right.end - right.start];
            right.buffer.getBytes(right.start, buf, 0, right.end - right.start);

            inputFormat = new DecimalFormat(new String(buf));

            java.text.DecimalFormatSymbols symbols = java.text.DecimalFormatSymbols.getInstance(java.util.Locale.getDefault(java.util.Locale.Category.FORMAT));

            String decimalSeparatorStr = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(decimalSeparator);
            if (decimalSeparatorStr != null && !decimalSeparatorStr.isEmpty()) {
                symbols.setDecimalSeparator(decimalSeparatorStr.charAt(0));
            }

            String groupSeparatorStr = org.apache.drill.exec.idvp.functions.FunctionsHelper.asString(groupSeparator);
            if (groupSeparatorStr != null && !groupSeparatorStr.isEmpty()) {
                symbols.setGroupingSeparator(groupSeparatorStr.charAt(0));
            }

            inputFormat = new java.text.DecimalFormat(new String(buf), symbols);


            decimalDigits = inputFormat.getMaximumFractionDigits();
        }

        public void eval() {
            byte[] buf1 = new byte[left.end - left.start];
            left.buffer.getBytes(left.start, buf1, 0, left.end - left.start);
            String input = new String(buf1);
            try {
                out.value = inputFormat.parse(input).doubleValue();
            }  catch(java.text.ParseException e) {
                throw new UnsupportedOperationException("Cannot parse input: " + input + " with pattern : " + inputFormat.toPattern());
            }

            // Round the value
            java.math.BigDecimal roundedValue = new java.math.BigDecimal(out.value);
            out.value = (roundedValue.setScale(decimalDigits, java.math.BigDecimal.ROUND_HALF_UP)).doubleValue();
        }
    }
}
