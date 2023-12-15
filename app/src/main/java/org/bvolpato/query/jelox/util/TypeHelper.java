/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bvolpato.query.jelox.util;

import io.substrait.type.Type;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TypeHelper {

  // TODO: add all types + consider all the attributes of the type
  public static FieldType getArrowFieldType(Type expressionType) {
    FieldType arrowType;
    if (expressionType instanceof Type.I64 i64) {
      arrowType = maybeNullable(new ArrowType.Int(32, true), expressionType.nullable());
    } else if (expressionType instanceof Type.I32 i32) {
      arrowType = maybeNullable(new ArrowType.Int(16, true), expressionType.nullable());
    } else if (expressionType instanceof Type.FP64 fp64) {
      arrowType =
          maybeNullable(
              new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
              expressionType.nullable());
    } else if (expressionType instanceof Type.Str str) {
      arrowType = maybeNullable(new ArrowType.Utf8(), expressionType.nullable());
    } else if (expressionType instanceof Type.VarChar str) {
      arrowType = maybeNullable(new ArrowType.Utf8(), expressionType.nullable());
    } else if (expressionType instanceof Type.FixedChar str) {
      arrowType = maybeNullable(new ArrowType.Utf8(), expressionType.nullable());
    } else {
      throw new RuntimeException("Unsupported type: " + expressionType);
    }
    return arrowType;
  }

  public static FieldType maybeNullable(ArrowType type, boolean nullable) {
    if (nullable) {
      return FieldType.nullable(type);
    } else {
      return FieldType.notNullable(type);
    }
  }
}
