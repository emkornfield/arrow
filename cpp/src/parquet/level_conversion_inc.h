// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include "parquet/level_conversion.h"

#include <algorithm>
#include <limits>
#if defined(ARROW_HAVE_BMI2)
#include <x86intrin.h>
#endif

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/level_comparison.h"

namespace parquet {
namespace internal {
namespace BMI_RUNTIME_VERSION {

using ::arrow::internal::BitRun;
using ::arrow::internal::BitRunReader;

/// Algorithm to simulate pext using BitRunReader for cases where all bits
/// not set or set.
uint64_t RunBasedExtractMixed(uint64_t bitmap, uint64_t select_bitmap) {
  bitmap = arrow::BitUtil::FromLittleEndian(bitmap);
  uint64_t new_bitmap = 0;
  ::arrow::internal::BitRunReader selection(reinterpret_cast<uint8_t*>(&select_bitmap),
                                            /*start_offset=*/0, /*length=*/64);
  ::arrow::internal::BitRun run = selection.NextRun();
  int64_t selected_bits = 0;
  while (run.length != 0) {
    if (run.set) {
      new_bitmap |= (bitmap & ::arrow::BitUtil::LeastSignficantBitMask(run.length))
                    << selected_bits;
      selected_bits += run.length;
    }
    bitmap = bitmap >> run.length;
    run = selection.NextRun();
  }
  return arrow::BitUtil::ToLittleEndian(new_bitmap);
}

inline uint64_t RunBasedExtractImpl(uint64_t bitmap, uint64_t select_bitmap) {
  /// These checks should be inline and are likely to be common cases.
  if (select_bitmap == ~uint64_t{0}) {
    return bitmap;
  } else if (select_bitmap == 0) {
    return 0;
  }
  /// Fallback to the slow method.
  return RunBasedExtractMixed(bitmap, select_bitmap);
}

inline uint64_t ExtractBits(uint64_t bitmap, uint64_t select_bitmap) {
#if defined(ARROW_HAVE_BMI2)
  return _pext_u64(bitmap, select_bitmap);
#else
  return RunBasedExtractImpl(bitmap, select_bitmap);
#endif
}

template <bool has_repeated_parent>
int64_t DefinitionLevelsBatchToBitmap(const int16_t* def_levels, const int64_t batch_size,
                                      LevelInfo level_info,
                                      ::arrow::internal::FirstTimeBitmapWriter* writer) {
  // Greater than level_info.def_level - 1 implies >= the def_level
  uint64_t defined_bitmap =
      internal::GreaterThanBitmap(def_levels, batch_size, level_info.def_level - 1);

  DCHECK_LE(batch_size, 64);
  if (has_repeated_parent) {
    // Greater than level_info.repeated_ancestor_def_level - 1 implies >= the
    // repeated_ancenstor_def_level
    uint64_t present_bitmap = internal::GreaterThanBitmap(
        def_levels, batch_size, level_info.repeated_ancestor_def_level - 1);
    uint64_t selected_bits = ExtractBits(defined_bitmap, present_bitmap);
    writer->AppendWord(selected_bits, ::arrow::BitUtil::PopCount(present_bitmap));
    return ::arrow::BitUtil::PopCount(selected_bits);
  } else {
    writer->AppendWord(defined_bitmap, batch_size);
    return ::arrow::BitUtil::PopCount(defined_bitmap);
  }
}

template <bool has_repeated_parent>
void DefinitionLevelsToBitmapSimd(const int16_t* def_levels, int64_t num_def_levels,
                                  LevelInfo level_info, int64_t* values_read,
                                  int64_t* null_count, uint8_t* valid_bits,
                                  int64_t valid_bits_offset) {
  constexpr int64_t kBitMaskSize = 64;
  ::arrow::internal::FirstTimeBitmapWriter writer(valid_bits,
                                                  /*start_offset=*/valid_bits_offset,
                                                  /*length=*/num_def_levels);
  int64_t set_count = 0;
  *values_read = 0;
  while (num_def_levels > kBitMaskSize) {
    set_count += DefinitionLevelsBatchToBitmap<has_repeated_parent>(
        def_levels, kBitMaskSize, level_info, &writer);
    def_levels += kBitMaskSize;
    num_def_levels -= kBitMaskSize;
  }
  set_count += DefinitionLevelsBatchToBitmap<has_repeated_parent>(
      def_levels, num_def_levels, level_info, &writer);

  *values_read = writer.position();
  *null_count += *values_read - set_count;
  writer.Finish();
}

}  // namespace BMI_RUNTIME_VERSION
}  // namespace internal
}  // namespace parquet
