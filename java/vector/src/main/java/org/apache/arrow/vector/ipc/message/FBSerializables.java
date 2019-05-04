/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.ipc.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.flatbuffers.FlatBufferBuilder;

public class FBSerializables {
  private FBSerializables() {}

  public static int writeAllStructsToVector(FlatBufferBuilder builder, List<? extends FBSerializable> all) {
    // struct vectors have to be created in reverse order
    List<? extends FBSerializable> reversed = new ArrayList<>(all);
    Collections.reverse(reversed);
    for (FBSerializable element : reversed) {
      element.writeTo(builder);
    }
    return builder.endVector();
  }
}
