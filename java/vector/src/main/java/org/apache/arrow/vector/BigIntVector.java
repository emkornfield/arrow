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

package org.apache.arrow.vector;

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.BigIntReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * BigIntVector implements a fixed width vector (8 bytes) of
 * integer values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class BigIntVector extends BaseFixedWidthVector implements BaseIntVector {
  public static final byte TYPE_WIDTH = 8;
  private final FieldReader reader;

  /**
   * Instantiate a BigIntVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public BigIntVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.BIGINT.getType()), allocator);
  }

  /**
   * Instantiate a BigIntVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public BigIntVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a BigIntVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public BigIntVector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
    reader = new BigIntReaderImpl(BigIntVector.this);
  }

  /**
   * Get a reader that supports reading values from this vector.
   *
   * @return Field Reader for this vector
   */
  @Override
  public FieldReader getReader() {
    return reader;
  }

  /**
   * Get minor type for this vector. The vector holds values belonging
   * to a particular type.
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.BIGINT;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public long get(long index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(long index, NullableBigIntHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(long)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public Long getObject(long index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getLong(index * TYPE_WIDTH);
    }
  }

  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  private void setValue(long index, long value) {
    valueBuffer.setLong(index * TYPE_WIDTH, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void set(long index, long value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(long index, NullableBigIntHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void set(long index, BigIntHolder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(long, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void setSafe(long index, long value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(long, NullableBigIntHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(long index, NullableBigIntHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(long, BigIntHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(long index, BigIntHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index   position of element
   */
  public void setNull(long index) {
    handleSafe(index);
    // not really needed to set the bit to 0 as long as
    // the buffer always starts from 0.
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void set(long index, int isSet, long value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int, long)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setSafe(long index, int isSet, long value) {
    handleSafe(index);
    set(index, isSet, value);
  }

  /**
   * Given a data buffer, get the value stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  public static long get(final ArrowBuf buffer, final long index) {
    return buffer.getLong(index * TYPE_WIDTH);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Construct a TransferPair comprising of this and and a target vector of
   * the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((BigIntVector) to);
  }

  @Override
  public void setWithPossibleTruncate(long index, long value) {
    this.setSafe(index, value);
  }

  @Override
  public void setUnsafeWithPossibleTruncate(long index, long value) {
    this.set(index, value);
  }

  @Override
  public long getValueAsLong(long index) {
    return this.get(index);
  }

  private class TransferImpl implements TransferPair {
    BigIntVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new BigIntVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(BigIntVector to) {
      this.to = to;
    }

    @Override
    public BigIntVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(long startIndex, long length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(long fromIndex, long toIndex) {
      to.copyFromSafe(fromIndex, toIndex, BigIntVector.this);
    }
  }
}
