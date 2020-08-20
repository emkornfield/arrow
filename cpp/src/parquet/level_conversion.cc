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
#include "parquet/level_conversion.h"

#include <algorithm>
#include <limits>
#if defined(ARROW_HAVE_BMI2)
#include <x86intrin.h>
#endif

#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "parquet/exception.h"

namespace parquet {
namespace internal {
namespace {
inline void CheckLevelRange(const int16_t* levels, int64_t num_levels,
                            const int16_t max_expected_level) {
  int16_t min_level = std::numeric_limits<int16_t>::max();
  int16_t max_level = std::numeric_limits<int16_t>::min();
  for (int x = 0; x < num_levels; x++) {
    min_level = std::min(levels[x], min_level);
    max_level = std::max(levels[x], max_level);
  }
  if (ARROW_PREDICT_FALSE(num_levels > 0 &&
                          (min_level < 0 || max_level > max_expected_level))) {
    throw ParquetException("definition level exceeds maximum");
  }
}

#if !defined(ARROW_HAVE_AVX512)

inline void DefinitionLevelsToBitmapScalar(
    const int16_t* def_levels, int64_t num_def_levels, const LevelInfo level_info, 
    int64_t* values_read, int64_t* null_count,
    uint8_t* valid_bits, int64_t valid_bits_offset) {
  // We assume here that valid_bits is large enough to accommodate the
  // additional definition levels and the ones that have already been written
  ::arrow::internal::BitmapWriter valid_bits_writer(valid_bits, valid_bits_offset,
                                                    num_def_levels);

  // TODO(itaiin): As an interim solution we are splitting the code path here
  // between repeated+flat column reads, and non-repeated+nested reads.
  // Those paths need to be merged in the future
  for (int i = 0; i < num_def_levels; ++i) {
    if (def_levels[i] == level_info.def_level) {
      valid_bits_writer.Set();
    } else if (def_levels[i] < level_info.repeated_ancestor_def_level {
      continue;
    } else if (def_levels[i] >= level_info.repeated_ancestor_def_level)  { 
      if (def_levels[i] <= level_info.max_def_level) {
        valid_bits_writer.Clear();
        *null_count += 1;
      } else {
        throw ParquetException("definition level exceeds maximum");
      }
    }
    valid_bits_writer.Next();
  }
  valid_bits_writer.Finish();
  *values_read = valid_bits_writer.position();
}
#endif

template <bool has_repeated_parent>
int64_t DefinitionLevelsBatchToBitmap(const int16_t* def_levels, const int64_t batch_size,
                                      const LevelInfo level_info, 
                                      ::arrow::internal::FirstTimeBitmapWriter* writer) {
  CheckLevelRange(def_levels, batch_size, required_definition_level);
  uint64_t defined_bitmap =
      internal::GreaterThanBitmap(def_levels, batch_size, level_info.def_level - 1);

  DCHECK_LE(batch_size, 64);
  if (has_repeated_parent) {
#if defined(ARROW_HAVE_BMI2)
    // repeated_ancestor_def_level denotes values present at >= its value.  so subtract
    // 1 to use the GreaterThan inequality.
    uint64_t present_bitmap = internal::GreaterThanBitmap(def_levels, batch_size,
                                                          level_info.repeated_ancestor_def_level - 1);
    uint64_t selected_bits = _pext_u64(defined_bitmap, present_bitmap);
    writer->AppendWord(selected_bits, ::arrow::BitUtil::PopCount(present_bitmap));
    return ::arrow::BitUtil::PopCount(selected_bits);
#else
    assert(false && "must not execute this without BMI2");
#endif
  } else {
    writer->AppendWord(defined_bitmap, batch_size);
    return ::arrow::BitUtil::PopCount(defined_bitmap);
  }
}

template <bool has_repeated_parent>
void DefinitionLevelsToBitmapSimd(const int16_t* def_levels, int64_t num_def_levels,
                                  const LevelInfo level_info, 
                                  int64_t* values_read, int64_t* null_count,
                                  uint8_t* valid_bits, int64_t valid_bits_offset) {
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

void DefinitionLevelsToBitmapLittleEndian(
    const int16_t* def_levels, int64_t num_def_levels, const LevelInfo level_info, 
    int64_t* values_read, int64_t* null_count,
    uint8_t* valid_bits, int64_t valid_bits_offset) {
  if (level_info.repeated_ancestor_def_level > 0) {
// This is a short term hack to prevent using the pext BMI2 instructions
// on non-intel platforms where performance is subpar.
// In the medium term we will hopefully be able to runtime dispatch
// to use this on intel only platforms that support pext.
#if defined(ARROW_HAVE_AVX512)
    // BMI2 is required for efficient bit extraction.
    DefinitionLevelsToBitmapSimd</*has_repeated_parent=*/true>(
        def_levels, num_def_levels, level_info, values_read, null_count,
        valid_bits, valid_bits_offset);
#else
    DefinitionLevelsToBitmapScalar(def_levels, num_def_levels, level_info,
                                   values_read, null_count,
                                   valid_bits, valid_bits_offset);
#endif  // ARROW_HAVE_BMI2

  } else {
    // No BMI2 intsturctions are used for non-repeated case.
    DefinitionLevelsToBitmapSimd</*has_repeated_parent=*/false>(
        def_levels, num_def_levels, level_meatadata, values_read, null_count,
        valid_bits, valid_bits_offset);
  }
}

}  // namespace

void DefinitionLevelsToBitmap(const int16_t* def_levels, int64_t num_def_levels,
		              const LevelInfo level_info,
                              int16_t max_repetition_level, int64_t* values_read,
                              int64_t* null_count, uint8_t* valid_bits,
                              int64_t valid_bits_offset) {
#if ARROW_LITTLE_ENDIAN
  DefinitionLevelsToBitmapLittleEndian(def_levels, num_def_levels, 
		  		       level_info,
                                       values_read, null_count,
                                       valid_bits, valid_bits_offset);

#else
  DefinitionLevelsToBitmapScalar(def_levels, num_def_levels, level_info, 
                                 values_read, null_count,
                                 valid_bits, valid_bits_offset);

#endif
  if (ARROW_PREDICT_FALSE(level_info.null_spacing > 1 && null_count > 0)) {
	// TODO(ARROW-9796): Support this case.
	throw ParquetException("Null values ucrrently unsuppored for FixedSizeLists.");
  }
}

int32_t* PARQUET_EXPORT RepLevelsToLengths(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    int32_t* lengths) {
 return  RepLevelsToLengthsScalar<int32_t>(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    int32_t* lengths);
}

template<class LengthType, bool inner_most, bool outer_most>
LengthType RepLevelsToLengthsScalar<int32_t>(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    LengthType* lengths) {
	for (int x = 0; x < num_def_rep_levels; x++) {
	 if (def_levels[x] >= level_info.repeated_ancestor_def_level) {
	  if (rep_levels[x] < level_info.rep_level) {
	    ++lengths;
	    *lengths = *(legths - 1) + (def_levels[x] > level_info.def_level ? 1 : 0);
	  } else if (inner_most || (rep_level[x] == level_info.rep_level)) {
	    ++*lengths;
	  }
	 }
    }
    return lengths;
}
}

// These prop
int64_t* PARQUET_EXPORT RepLevelsToLengths(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    int64_t* lengths) {
	if (level_info.inner_most_list()) {
return  RepLevelsToLengthsScalar<int32_t, /*inner_most=*/true>(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    int32_t* lengths);
	} else {
return  RepLevelsToLengthsScalar<int32_t, /*inner_most=*/false>(
    const int16_t* def_levels, const int16_t* rep_levels, 
    int64_t num_def_rep_levels, const LevelInfo level_info, 
    int32_t* lengths);

	}
}



}  // namespace internal
}  // namespace parquet
