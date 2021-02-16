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

#include "arrow/csv/writer.h"
#include "arrow/array.h"
#include "arrow/compute/cast.h"
#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/result_internal.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/make_unique.h"

#include "arrow/visitor_inline.h"

namespace arrow {
namespace csv {
// This implementation is intentionally light on configurability to minimize the size of
// the initial PR. Aditional features can be added as there is demand and interest to
// implement them.
//
// The algorithm used here at a high level is to break RecordBatches/Tables into slices
// and convert each slice independently.  A slice is then converted to CSV by first
// scanning each column to determine the size of its contents when rendered as a string in
// CSV. For non-string types this requires casting the value to string (which is cached).
// This data is used to understand the precise length of each row and a single allocation
// for the final CSV data buffer. Once the final size is known each column is then
// iterated over again to place its contents into the CSV data buffer. The rationale for
// choosing this approach is it allows for reuse of the cast functionality in the compute
// module // and inline data visiting functionality in the core library. A performance
// comparison has not been done using a naive single-pass approach. This approach might
// still be competitive due to reduction in the number of per row branches necessary with
// a single pass approach. Profiling would likely yield further opportunities for
// optimization with this approach.

namespace {

// Counts the number of characters that need escaping in s.
int64_t CountEscapes(util::string_view s) {
  return static_cast<int64_t>(std::count(s.begin(), s.end(), '"'));
}

// Matching quote pair character length.
constexpr int64_t kQuoteCount = 2;

// Interface for generating CSV data per column.
// The intended usage is to iteratively call UpdateRowLengths for a column and
// then PopulateColumns.
class ColumnPopulator {
 public:
  ColumnPopulator(MemoryPool* pool, char end_char) : end_char_(end_char), pool_(pool) {}
  virtual ~ColumnPopulator() = default;
  // Adds the number of characters each entry in data will add to to elements
  // in row_lengths.
  Status UpdateRowLengths(const Array& data, int32_t* row_lengths) {
    compute::ExecContext ctx(pool_);
    // Populators are intented to be applied to reasonably small data.  In most cases
    // threading overhead would not be justified.
    ctx.set_use_threads(false);
    ASSIGN_OR_RAISE(
        std::shared_ptr<Array> casted,
        compute::Cast(data, /*to_type=*/utf8(), compute::CastOptions(), &ctx));
    casted_array_ = internal::checked_pointer_cast<StringArray>(casted);
    return UpdateRowLengths(row_lengths);
  }

  // Places string data onto each row in output and updates the corresponding row
  // row pointers in preparation for calls to other ColumnPopulators.
  virtual void PopulateColumns(char** output) const = 0;

 protected:
  virtual Status UpdateRowLengths(int32_t* row_lengths) = 0;
  std::shared_ptr<StringArray> casted_array_;
  const char end_char_;

 private:
  MemoryPool* const pool_;
};

// Copies the contents of to out properly escaping any necessary charaters.
char* Escape(arrow::util::string_view s, char* out) {
  for (const char* val = s.data(); val < s.data() + s.length(); val++, out++) {
    if (*val == '"') {
      *out = *val;
      out++;
    }
    *out = *val;
  }
  return out;
}

// Populator for non-string types.  This populator relies on compute Cast functionality to
// String if it doesn't exist it will be an error.  it also assumes the resulting string
// from a cast does not require quoting or escaping.
class UnquotedColumnPopulator : public ColumnPopulator {
 public:
  explicit UnquotedColumnPopulator(MemoryPool* memory_pool, char end_char)
      : ColumnPopulator(memory_pool, end_char) {}
  Status UpdateRowLengths(int32_t* row_lengths) override {
    for (int x = 0; x < casted_array_->length(); x++) {
      row_lengths[x] += casted_array_->value_length(x);
    }
    return Status::OK();
  }

  void PopulateColumns(char** rows) const override {
    VisitArrayDataInline<StringType>(
        *casted_array_->data(),
        [&](arrow::util::string_view s) {
          int64_t next_column_offset = s.length() + /*end_char*/ 1;
          memcpy(*rows, s.data(), s.length());
          *(*rows + s.length()) = end_char_;
          *rows += next_column_offset;
          rows++;
        },
        [&]() {
          // Nulls are empty (unquoted) to distinguish with empty string.
          **rows = end_char_;
          *rows += 1;
          rows++;
        });
  }
};

// Strings need special handling to ensure they are escaped properly.
// This class handles escaping assuming that all strings will be quoted
// and that the only character within the string that needs to escaped is
// a quote character (") and escaping is done my adding another quote.
class QuotedColumnPopulator : public ColumnPopulator {
 public:
  QuotedColumnPopulator(MemoryPool* pool, char end_char)
      : ColumnPopulator(pool, end_char) {}

  Status UpdateRowLengths(int32_t* row_lengths) override {
    const StringArray& input = *casted_array_;
    extra_chars_count_.resize(input.length());
    auto extra_chars = extra_chars_count_.begin();
    VisitArrayDataInline<StringType>(
        *input.data(),
        [&](arrow::util::string_view s) {
          int64_t escaped_count = CountEscapes(s);
          // TODO: Maybe use 64 bit row lengths or safe cast?
          *extra_chars = static_cast<int>(escaped_count) + kQuoteCount;
          extra_chars++;
        },
        [&]() {
          *extra_chars = 0;
          extra_chars++;
        });

    for (int x = 0; x < input.length(); x++) {
      row_lengths[x] += extra_chars_count_[x] + input.value_length(x);
    }
    return Status::OK();
  }

  void PopulateColumns(char** rows) const override {
    const int32_t* extra_chars = extra_chars_count_.data();
    VisitArrayDataInline<StringType>(
        *(casted_array_->data()),
        [&](arrow::util::string_view s) {
          int64_t next_column_offset = *extra_chars + s.length() + /*end_char*/ 1;
          **rows = '"';
          if (*extra_chars == kQuoteCount) {
            memcpy((*rows + 1), s.data(), s.length());
          } else {
            Escape(s, (*rows + 1));
          }
          *(*rows + next_column_offset - 2) = '"';
          *(*rows + next_column_offset - 1) = end_char_;
          *rows += next_column_offset;
          extra_chars++;
          rows++;
        },
        [&]() {
          // Nulls are empty (unquoted) to distinguish with empty string.
          **rows = end_char_;
          *rows += 1;
          rows++;
          extra_chars++;
        });
  }

 private:
  std::vector<int32_t, std::allocator<int32_t>> extra_chars_count_;
};

struct PopulatorFactory {
  template <typename TypeClass>
  enable_if_t<is_base_binary_type<TypeClass>::value ||
                  std::is_same<FixedSizeBinaryType, TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    populator = new QuotedColumnPopulator(pool, end_char);
    return Status::OK();
  }

  template <typename TypeClass>
  enable_if_dictionary<TypeClass, Status> Visit(const TypeClass& type) {
    return VisitTypeInline(*type.value_type(), this);
  }

  template <typename TypeClass>
  enable_if_t<is_nested_type<TypeClass>::value || is_extension_type<TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    return Status::Invalid("Nested and extension types not supported");
  }

  template <typename TypeClass>
  enable_if_t<is_primitive_ctype<TypeClass>::value || is_decimal_type<TypeClass>::value ||
                  is_null_type<TypeClass>::value || is_temporal_type<TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    populator = new UnquotedColumnPopulator(pool, end_char);
    return Status::OK();
  }

  char end_char;
  MemoryPool* pool;
  ColumnPopulator* populator;
};

Result<std::unique_ptr<ColumnPopulator>> MakePopulator(const Field& field, char end_char,
                                                       MemoryPool* pool) {
  PopulatorFactory factory{end_char, pool, nullptr};
  RETURN_NOT_OK(VisitTypeInline(*field.type(), &factory));
  return std::unique_ptr<ColumnPopulator>(factory.populator);
}

class CsvConverter {
 public:
  static Result<std::unique_ptr<CsvConverter>> Make(std::shared_ptr<Schema> schema,
                                                    MemoryPool* pool) {
    std::vector<std::unique_ptr<ColumnPopulator>> populators(schema->num_fields());
    for (int col = 0; col < schema->num_fields(); col++) {
      char end_char = col < schema->num_fields() - 1 ? ',' : '\n';
      ASSIGN_OR_RAISE(populators[col],
                      MakePopulator(*schema->field(col), end_char, pool));
    }
    return std::unique_ptr<CsvConverter>(
        new CsvConverter(std::move(schema), std::move(populators), pool));
  }
  static constexpr int64_t kColumnSizeGuess = 8;
  Status WriteCsv(const RecordBatch& batch, const WriteOptions& options,
                  io::OutputStream* out) {
    RETURN_NOT_OK(PrepareForContentsWrite(options, out));
    RecordBatchIterator iterator = batch.SliceIterator(options.batch_size);
    for (auto maybe_slice : iterator) {
      ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> slice, maybe_slice);
      RETURN_NOT_OK(TranslateMininalBatch(*slice));
      RETURN_NOT_OK(out->Write(data_buffer_));
    }
    return Status::OK();
  }

  Status WriteCsv(const Table& table, const WriteOptions& options,
                  io::OutputStream* out) {
    TableBatchReader reader(table);
    reader.set_chunksize(options.batch_size);
    RETURN_NOT_OK(PrepareForContentsWrite(options, out));
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader.ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(TranslateMininalBatch(*batch));
      RETURN_NOT_OK(out->Write(data_buffer_));
      RETURN_NOT_OK(reader.ReadNext(&batch));
    }

    return Status::OK();
  }

 private:
  CsvConverter(std::shared_ptr<Schema> schema,
               std::vector<std::unique_ptr<ColumnPopulator>> populators, MemoryPool* pool)
      : schema_(std::move(schema)),
        column_populators_(std::move(populators)),
        row_positions_(1024, nullptr, arrow::stl::allocator<char*>(pool)),
        pool_(pool) {}

  const std::shared_ptr<Schema> schema_;

  Status PrepareForContentsWrite(const WriteOptions& options, io::OutputStream* out) {
    if (data_buffer_ == nullptr) {
      ASSIGN_OR_RAISE(
          data_buffer_,
          AllocateResizableBuffer(
              options.batch_size * schema_->num_fields() * kColumnSizeGuess, pool_));
    }
    if (options.include_header) {
      RETURN_NOT_OK(WriteHeader(out));
    }
    return Status::OK();
  }

  int64_t CalculateHeaderSize() const {
    int64_t header_length = 0;
    for (int col = 0; col < schema_->num_fields(); col++) {
      const std::string& col_name = schema_->field(col)->name();
      header_length += col_name.size();
      header_length += CountEscapes(col_name);
    }
    return header_length + (3 * schema_->num_fields());
  }

  Status WriteHeader(io::OutputStream* out) {
    RETURN_NOT_OK(data_buffer_->Resize(CalculateHeaderSize(), /*shrink_to_fit=*/false));
    char* next = reinterpret_cast<char*>(data_buffer_->mutable_data());
    for (int col = 0; col < schema_->num_fields(); col++) {
      *next++ = '"';
      next = Escape(schema_->field(col)->name(), next);
      *next++ = '"';
      *next++ = ',';
    }
    next--;
    *next = '\n';
    return out->Write(data_buffer_);
  }

  Status TranslateMininalBatch(const RecordBatch& batch) {
    if (batch.num_rows() == 0) {
      return Status::OK();
    }
    std::vector<int32_t, arrow::stl::allocator<int32_t>> offsets(
        batch.num_rows(), 0, arrow::stl::allocator<int32_t>(pool_));

    // Calculate relative offsets for each row (excluding delimiters)
    for (size_t col = 0; col < column_populators_.size(); col++) {
      RETURN_NOT_OK(
          column_populators_[col]->UpdateRowLengths(*batch.column(col), offsets.data()));
    }
    // Calculate cumulalative offsets for each row (including delimiters).
    offsets[0] += batch.num_columns();
    for (int64_t row = 1; row < batch.num_rows(); row++) {
      offsets[row] += offsets[row - 1] + /*delimiter lengths*/ batch.num_columns();
    }
    // Resize the target buffer to required size. We assume batch to batch sizes
    // should be pretty close so don't shrink the buffer to avoid allocation churn.
    RETURN_NOT_OK(data_buffer_->Resize(offsets.back(), /*shrink_to_fit=*/false));

    // Calculate pointers to the start of each row.
    row_positions_.resize(batch.num_rows());
    row_positions_[0] = reinterpret_cast<char*>(data_buffer_->mutable_data());
    for (size_t row = 1; row < row_positions_.size(); row++) {
      row_positions_[row] =
          reinterpret_cast<char*>(data_buffer_->mutable_data()) + offsets[row - 1];
    }
    // Use the pointers to populate all of the data.
    for (const auto& populator : column_populators_) {
      populator->PopulateColumns(row_positions_.data());
    }
    return Status::OK();
  }
  std::vector<std::unique_ptr<ColumnPopulator>> column_populators_;
  std::vector<char*, arrow::stl::allocator<char*>> row_positions_;
  std::shared_ptr<ResizableBuffer> data_buffer_;
  MemoryPool* pool_;
};

}  // namespace

Status WriteCsv(const Table& table, const WriteOptions& options, MemoryPool* pool,
                arrow::io::OutputStream* output) {
  if (pool == nullptr) {
    pool = default_memory_pool();
  }
  ASSIGN_OR_RAISE(std::unique_ptr<CsvConverter> converter,
                  CsvConverter::Make(table.schema(), pool));
  return converter->WriteCsv(table, options, output);
}

Status WriteCsv(const RecordBatch& batch, const WriteOptions& options, MemoryPool* pool,
                arrow::io::OutputStream* output) {
  if (pool == nullptr) {
    pool = default_memory_pool();
  }

  ASSIGN_OR_RAISE(std::unique_ptr<CsvConverter> converter,
                  CsvConverter::Make(batch.schema(), pool));
  return converter->WriteCsv(batch, options, output);
}

}  // namespace csv
}  // namespace arrow
