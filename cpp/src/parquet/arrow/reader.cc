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

#include "parquet/arrow/reader.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/range.h"
#include "arrow/util/thread_pool.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"

using arrow::Array;
using arrow::ArrayData;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Field;
using arrow::Future;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::RecordBatchReader;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;
using arrow::internal::Iota;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;

using parquet::internal::RecordReader;

#define BEGIN_PARQUET_CATCH_EXCEPTIONS try {
#define END_PARQUET_CATCH_EXCEPTIONS             \
  }                                              \
  catch (const ::parquet::ParquetException& e) { \
    return ::arrow::Status::IOError(e.what());   \
  }

namespace parquet {
namespace arrow {
namespace {

::arrow::Result<std::shared_ptr<ArrayData>> ChunksToSingle(const ChunkedArray& chunked) {
  switch (chunked.num_chunks()) {
    case 0: {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> array,
                            ::arrow::MakeArrayOfNull(chunked.type(), 0));
      return array->data();
    }
    case 1:
      return chunked.chunk(0)->data();
    default:
      // ARROW-3762(wesm): If item reader yields a chunked array, we reject as
      // this is not yet implemented
      return Status::NotImplemented(
          "Nested data conversions not implemented for chunked array outputs");
  }
}

}  // namespace

class ColumnReaderImpl : public ColumnReader {
 public:
  virtual Status GetDefLevels(const int16_t** data, int64_t* length) = 0;
  virtual Status GetRepLevels(const int16_t** data, int64_t* length) = 0;
  virtual const std::shared_ptr<Field> field() = 0;
  ::arrow::Status NextBatch(int64_t batch_size,
                            std::shared_ptr<::arrow::ChunkedArray>* out) final {
    RETURN_NOT_OK(LoadBatch(batch_size));
    return BuildArray(batch_size, out);
  }

  virtual ::arrow::Status LoadBatch(int64_t num_records) = 0;

  virtual ::arrow::Status BuildArray(int64_t number_of_slots,
                                     std::shared_ptr<::arrow::ChunkedArray>* out) = 0;
  virtual bool IsOrHasRepeatedChild() const = 0;
};

std::shared_ptr<std::unordered_set<int>> VectorToSharedSet(
    const std::vector<int>& values) {
  std::shared_ptr<std::unordered_set<int>> result(new std::unordered_set<int>());
  result->insert(values.begin(), values.end());
  return result;
}

// ----------------------------------------------------------------------
// FileReaderImpl forward declaration

class FileReaderImpl : public FileReader {
 public:
  FileReaderImpl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader,
                 ArrowReaderProperties properties)
      : pool_(pool),
        reader_(std::move(reader)),
        reader_properties_(std::move(properties)) {}

  Status Init() {
    return SchemaManifest::Make(reader_->metadata()->schema(),
                                reader_->metadata()->key_value_metadata(),
                                reader_properties_, &manifest_);
  }

  FileColumnIteratorFactory SomeRowGroupsFactory(std::vector<int> row_groups) {
    return [row_groups](int i, ParquetFileReader* reader) {
      return new FileColumnIterator(i, reader, row_groups);
    };
  }

  FileColumnIteratorFactory AllRowGroupsFactory() {
    return SomeRowGroupsFactory(Iota(reader_->metadata()->num_row_groups()));
  }

  Status BoundsCheckColumn(int column) {
    if (column < 0 || column >= this->num_columns()) {
      return Status::Invalid("Column index out of bounds (got ", column,
                             ", should be "
                             "between 0 and ",
                             this->num_columns() - 1, ")");
    }
    return Status::OK();
  }

  Status BoundsCheckRowGroup(int row_group) {
    // row group indices check
    if (row_group < 0 || row_group >= num_row_groups()) {
      return Status::Invalid("Some index in row_group_indices is ", row_group,
                             ", which is either < 0 or >= num_row_groups(",
                             num_row_groups(), ")");
    }
    return Status::OK();
  }

  Status BoundsCheck(const std::vector<int>& row_groups,
                     const std::vector<int>& column_indices) {
    for (int i : row_groups) {
      return BoundsCheckRowGroup(i);
    }
    for (int i : column_indices) {
      return BoundsCheckColumn(i);
    }
    return Status::OK();
  }

  std::shared_ptr<RowGroupReader> RowGroup(int row_group_index) override;

  Status ReadTable(const std::vector<int>& indices,
                   std::shared_ptr<Table>* out) override {
    return ReadRowGroups(Iota(reader_->metadata()->num_row_groups()), indices, out);
  }

  Status GetFieldReader(int i,
                        const std::shared_ptr<std::unordered_set<int>>& included_leaves,
                        const std::vector<int>& row_groups,
                        std::unique_ptr<ColumnReaderImpl>* out) {
    auto ctx = std::make_shared<ReaderContext>();
    ctx->reader = reader_.get();
    ctx->pool = pool_;
    ctx->iterator_factory = SomeRowGroupsFactory(row_groups);
    ctx->filter_leaves = true;
    ctx->included_leaves = included_leaves;
    return GetReader(manifest_.schema_fields[i], ctx, out);
  }

  Status GetFieldReaders(const std::vector<int>& column_indices,
                         const std::vector<int>& row_groups,
                         std::vector<std::shared_ptr<ColumnReaderImpl>>* out,
                         std::shared_ptr<::arrow::Schema>* out_schema) {
    // We only need to read schema fields which have columns indicated
    // in the indices vector
    ARROW_ASSIGN_OR_RAISE(std::vector<int> field_indices,
                          manifest_.GetFieldIndices(column_indices));

    auto included_leaves = VectorToSharedSet(column_indices);

    out->resize(field_indices.size());
    ::arrow::FieldVector out_fields(field_indices.size());
    for (size_t i = 0; i < out->size(); ++i) {
      std::unique_ptr<ColumnReaderImpl> reader;
      RETURN_NOT_OK(
          GetFieldReader(field_indices[i], included_leaves, row_groups, &reader));

      out_fields[i] = reader->field();
      out->at(i) = std::move(reader);
    }

    *out_schema = ::arrow::schema(std::move(out_fields), manifest_.schema_metadata);
    return Status::OK();
  }

  Status GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                   std::unique_ptr<ColumnReader>* out);

  Status GetColumn(int i, std::unique_ptr<ColumnReader>* out) override {
    return GetColumn(i, AllRowGroupsFactory(), out);
  }

  Status GetSchema(std::shared_ptr<::arrow::Schema>* out) override {
    return FromParquetSchema(reader_->metadata()->schema(), reader_properties_,
                             reader_->metadata()->key_value_metadata(), out);
  }

  Status ReadSchemaField(int i, std::shared_ptr<ChunkedArray>* out) override {
    auto included_leaves = VectorToSharedSet(Iota(reader_->metadata()->num_columns()));
    std::vector<int> row_groups = Iota(reader_->metadata()->num_row_groups());

    std::unique_ptr<ColumnReaderImpl> reader;
    RETURN_NOT_OK(GetFieldReader(i, included_leaves, row_groups, &reader));

    return ReadColumn(i, row_groups, reader.get(), out);
  }

  Status ReadColumn(int i, const std::vector<int>& row_groups, ColumnReader* reader,
                    std::shared_ptr<ChunkedArray>* out) {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    // TODO(wesm): This calculation doesn't make much sense when we have repeated
    // schema nodes
    int64_t records_to_read = 0;
    for (auto row_group : row_groups) {
      // Can throw exception
      records_to_read +=
          reader_->metadata()->RowGroup(row_group)->ColumnChunk(i)->num_values();
    }
    return reader->NextBatch(records_to_read, out);
    END_PARQUET_CATCH_EXCEPTIONS
  }

  Status ReadColumn(int i, const std::vector<int>& row_groups,
                    std::shared_ptr<ChunkedArray>* out) {
    std::unique_ptr<ColumnReader> flat_column_reader;
    RETURN_NOT_OK(GetColumn(i, SomeRowGroupsFactory(row_groups), &flat_column_reader));
    return ReadColumn(i, row_groups, flat_column_reader.get(), out);
  }

  Status ReadColumn(int i, std::shared_ptr<ChunkedArray>* out) override {
    return ReadColumn(i, Iota(reader_->metadata()->num_row_groups()), out);
  }

  Status ReadTable(std::shared_ptr<Table>* table) override {
    return ReadTable(Iota(reader_->metadata()->num_columns()), table);
  }

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       const std::vector<int>& indices,
                       std::shared_ptr<Table>* table) override;

  Status ReadRowGroups(const std::vector<int>& row_groups,
                       std::shared_ptr<Table>* table) override {
    return ReadRowGroups(row_groups, Iota(reader_->metadata()->num_columns()), table);
  }

  Status ReadRowGroup(int row_group_index, const std::vector<int>& column_indices,
                      std::shared_ptr<Table>* out) override {
    return ReadRowGroups({row_group_index}, column_indices, out);
  }

  Status ReadRowGroup(int i, std::shared_ptr<Table>* table) override {
    return ReadRowGroup(i, Iota(reader_->metadata()->num_columns()), table);
  }

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              const std::vector<int>& column_indices,
                              std::unique_ptr<RecordBatchReader>* out) override;

  Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                              std::unique_ptr<RecordBatchReader>* out) override {
    return GetRecordBatchReader(row_group_indices,
                                Iota(reader_->metadata()->num_columns()), out);
  }

  int num_columns() const { return reader_->metadata()->num_columns(); }

  ParquetFileReader* parquet_reader() const override { return reader_.get(); }

  int num_row_groups() const override { return reader_->metadata()->num_row_groups(); }

  void set_use_threads(bool use_threads) override {
    reader_properties_.set_use_threads(use_threads);
  }

  const ArrowReaderProperties& properties() const override { return reader_properties_; }

  const SchemaManifest& manifest() const override { return manifest_; }

  Status ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                      int64_t* num_rows) override {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    *num_rows = ScanFileContents(columns, column_batch_size, reader_.get());
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

  MemoryPool* pool_;
  std::unique_ptr<ParquetFileReader> reader_;
  ArrowReaderProperties reader_properties_;

  SchemaManifest manifest_;
};

class RowGroupRecordBatchReader : public ::arrow::RecordBatchReader {
 public:
  RowGroupRecordBatchReader(::arrow::RecordBatchIterator batches,
                            std::shared_ptr<::arrow::Schema> schema)
      : batches_(std::move(batches)), schema_(std::move(schema)) {}

  ~RowGroupRecordBatchReader() override {}

  Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) override {
    return batches_.Next().Value(out);
  }

  std::shared_ptr<::arrow::Schema> schema() const override { return schema_; }

 private:
  ::arrow::Iterator<std::shared_ptr<::arrow::RecordBatch>> batches_;
  std::shared_ptr<::arrow::Schema> schema_;
};

class ColumnChunkReaderImpl : public ColumnChunkReader {
 public:
  ColumnChunkReaderImpl(FileReaderImpl* impl, int row_group_index, int column_index)
      : impl_(impl), column_index_(column_index), row_group_index_(row_group_index) {}

  Status Read(std::shared_ptr<::arrow::ChunkedArray>* out) override {
    return impl_->ReadColumn(column_index_, {row_group_index_}, out);
  }

 private:
  FileReaderImpl* impl_;
  int column_index_;
  int row_group_index_;
};

class RowGroupReaderImpl : public RowGroupReader {
 public:
  RowGroupReaderImpl(FileReaderImpl* impl, int row_group_index)
      : impl_(impl), row_group_index_(row_group_index) {}

  std::shared_ptr<ColumnChunkReader> Column(int column_index) override {
    return std::shared_ptr<ColumnChunkReader>(
        new ColumnChunkReaderImpl(impl_, row_group_index_, column_index));
  }

  Status ReadTable(const std::vector<int>& column_indices,
                   std::shared_ptr<::arrow::Table>* out) override {
    return impl_->ReadRowGroup(row_group_index_, column_indices, out);
  }

  Status ReadTable(std::shared_ptr<::arrow::Table>* out) override {
    return impl_->ReadRowGroup(row_group_index_, out);
  }

 private:
  FileReaderImpl* impl_;
  int row_group_index_;
};

// Leaf reader is for primitive arrays and primitive children of nested arrays
class LeafReader : public ColumnReaderImpl {
 public:
  LeafReader(std::shared_ptr<ReaderContext> ctx, std::shared_ptr<Field> field,
             std::unique_ptr<FileColumnIterator> input,
             ::parquet::internal::LevelInfo leaf_info)
      : ctx_(std::move(ctx)),
        field_(std::move(field)),
        input_(std::move(input)),
        descr_(input_->descr()) {
    record_reader_ = RecordReader::Make(
        descr_, leaf_info, ctx_->pool, field_->type()->id() == ::arrow::Type::DICTIONARY);
    NextRowGroup();
  }

  Status GetDefLevels(const int16_t** data, int64_t* length) final {
    *data = record_reader_->def_levels();
    *length = record_reader_->levels_position();
    return Status::OK();
  }

  Status GetRepLevels(const int16_t** data, int64_t* length) final {
    *data = record_reader_->rep_levels();
    *length = record_reader_->levels_position();
    return Status::OK();
  }

  bool IsOrHasRepeatedChild() const final { return false; }

  Status LoadBatch(int64_t records_to_read) final {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    out_ = nullptr;
    record_reader_->Reset();
    // Pre-allocation gives much better performance for flat columns
    record_reader_->Reserve(records_to_read);
    while (records_to_read > 0) {
      if (!record_reader_->HasMoreData()) {
        break;
      }
      int64_t records_read = record_reader_->ReadRecords(records_to_read);
      records_to_read -= records_read;
      if (records_read == 0) {
        NextRowGroup();
      }
    }
    RETURN_NOT_OK(TransferColumnData(record_reader_.get(), field_->type(), descr_,
                                     ctx_->pool, &out_));
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  }

  ::arrow::Status BuildArray(int64_t expected_array_entries,
                             std::shared_ptr<::arrow::ChunkedArray>* out) final {
    *out = out_;
    return Status::OK();
  }

  const std::shared_ptr<Field> field() override { return field_; }

 private:
  std::shared_ptr<ChunkedArray> out_;
  void NextRowGroup() {
    std::unique_ptr<PageReader> page_reader = input_->NextChunk();
    record_reader_->SetPageReader(std::move(page_reader));
  }

  std::shared_ptr<ReaderContext> ctx_;
  std::shared_ptr<Field> field_;
  std::unique_ptr<FileColumnIterator> input_;
  const ColumnDescriptor* descr_;
  std::shared_ptr<RecordReader> record_reader_;
};

template <typename IndexType>
class ListReader : public ColumnReaderImpl {
 public:
  ListReader(std::shared_ptr<ReaderContext> ctx, std::shared_ptr<Field> field,
             ::parquet::internal::LevelInfo level_info,
             std::unique_ptr<ColumnReaderImpl> child_reader)
      : ctx_(std::move(ctx)),
        field_(std::move(field)),
        level_info_(level_info),
        item_reader_(std::move(child_reader)) {}

  Status GetDefLevels(const int16_t** data, int64_t* length) override {
    return item_reader_->GetDefLevels(data, length);
  }

  Status GetRepLevels(const int16_t** data, int64_t* length) override {
    return item_reader_->GetRepLevels(data, length);
  }

  bool IsOrHasRepeatedChild() const final { return true; }

  Status LoadBatch(int64_t number_of_records) final {
    return item_reader_->LoadBatch(number_of_records);
  }

  Status BuildArray(int64_t number_of_slots,
                    std::shared_ptr<ChunkedArray>* out) override {
    const int16_t* def_levels;
    const int16_t* rep_levels;
    int64_t num_levels;
    RETURN_NOT_OK(item_reader_->GetDefLevels(&def_levels, &num_levels));
    RETURN_NOT_OK(item_reader_->GetRepLevels(&rep_levels, &num_levels));
    std::shared_ptr<Buffer> validity_buffer;
    ::parquet::internal::ValidityBitmapInputOutput validity_io;
    validity_io.values_read_upper_bound = number_of_slots;
    if (field_->nullable()) {
      ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBitmap(number_of_slots, ctx_->pool));

      validity_io.valid_bits = validity_buffer->mutable_data();
    }
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Buffer> lengths_buffer,
        AllocateBuffer(sizeof(IndexType) * std::max(int64_t{2}, number_of_slots + 1),
                       ctx_->pool));
    // ensure zero initialization in case we have reached a zero length list (and
    // because first entry is always zero).
    IndexType* length_data = reinterpret_cast<IndexType*>(lengths_buffer->mutable_data());
    std::fill(length_data, length_data + 2, 0);
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    ::parquet::internal::DefRepLevelsToList(def_levels, rep_levels, num_levels,
                                            level_info_, &validity_io, length_data);
    END_PARQUET_CATCH_EXCEPTIONS
    // We might return less then the requested slot (i.e. reaching an end of a file)
    // ensure we've set all the bits here.
    if (validity_io.values_read < number_of_slots) {
      // + 1  because arrays lengths are values + 1
      std::fill(length_data + validity_io.values_read + 1,
                length_data + number_of_slots + 1, 0);
      if (validity_io.valid_bits != nullptr &&
          BitUtil::BytesForBits(validity_io.values_read) <
              BitUtil::BytesForBits(number_of_slots)) {
        std::fill(validity_io.valid_bits + BitUtil::BytesForBits(validity_io.values_read),
                  validity_io.valid_bits + BitUtil::BytesForBits(number_of_slots), 0);
      }
    }

    RETURN_NOT_OK(item_reader_->BuildArray(length_data[validity_io.values_read], out));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> item_chunk, ChunksToSingle(**out));

    std::vector<std::shared_ptr<Buffer>> buffers{
        validity_io.null_count > 0 ? validity_buffer : std::shared_ptr<Buffer>(),
        lengths_buffer};
    auto data = std::make_shared<ArrayData>(
        field_->type(),
        /*length=*/validity_io.values_read, std::move(buffers),
        std::vector<std::shared_ptr<ArrayData>>{item_chunk}, validity_io.null_count);

    std::shared_ptr<Array> result = ::arrow::MakeArray(data);
    RETURN_NOT_OK(result->Validate());
    *out = std::make_shared<ChunkedArray>(result);
    return Status::OK();
  }

  const std::shared_ptr<Field> field() override { return field_; }

 private:
  std::shared_ptr<ReaderContext> ctx_;
  std::shared_ptr<Field> field_;
  ::parquet::internal::LevelInfo level_info_;
  std::unique_ptr<ColumnReaderImpl> item_reader_;
};

class PARQUET_NO_EXPORT StructReader : public ColumnReaderImpl {
 public:
  explicit StructReader(std::shared_ptr<ReaderContext> ctx,
                        std::shared_ptr<Field> filtered_field,
                        ::parquet::internal::LevelInfo level_info,
                        std::vector<std::unique_ptr<ColumnReaderImpl>> children)
      : ctx_(std::move(ctx)),
        filtered_field_(std::move(filtered_field)),
        level_info_(level_info),
        children_(std::move(children)) {
    // There could be a mix of children some might be repeated some might not be.
    // If possible use one that isn't since that will be guaranteed to have the least
    // number of rep/def levels.
    auto result = std::find_if(children_.begin(), children_.end(),
                               [](const std::unique_ptr<ColumnReaderImpl>& child) {
                                 return !child->IsOrHasRepeatedChild();
                               });
    if (result != children_.end()) {
      def_rep_level_child_ = result->get();
      has_repeated_child_ = false;
    } else if (!children_.empty()) {
      def_rep_level_child_ = children_.front().get();
      has_repeated_child_ = true;
    }
  }

  bool IsOrHasRepeatedChild() const final { return has_repeated_child_; }

  Status LoadBatch(int64_t records_to_read) override {
    for (const std::unique_ptr<ColumnReaderImpl>& reader : children_) {
      RETURN_NOT_OK(reader->LoadBatch(records_to_read));
    }
    return Status::OK();
  }
  Status BuildArray(int64_t records_to_read, std::shared_ptr<ChunkedArray>* out) override;
  Status GetDefLevels(const int16_t** data, int64_t* length) override;
  Status GetRepLevels(const int16_t** data, int64_t* length) override;
  const std::shared_ptr<Field> field() override { return filtered_field_; }

 private:
  const std::shared_ptr<ReaderContext> ctx_;
  const std::shared_ptr<Field> filtered_field_;
  const ::parquet::internal::LevelInfo level_info_;
  const std::vector<std::unique_ptr<ColumnReaderImpl>> children_;
  ColumnReaderImpl* def_rep_level_child_ = nullptr;
  bool has_repeated_child_;
};

Status StructReader::GetDefLevels(const int16_t** data, int64_t* length) {
  *data = nullptr;
  if (children_.size() == 0) {
    *length = 0;
    return Status::Invalid("StructReader had no childre");
  }

  // This method should only be called when this struct or one of its parents
  // are optional/repeated or it has a repeated child.
  // Meaning all children must have rep/def levels associated
  // with them.
  RETURN_NOT_OK(def_rep_level_child_->GetDefLevels(data, length));

  if (*data == nullptr) {
    // Only happens if there are actually 0 rows available.
    *data = nullptr;
    *length = 0;
  }
  return Status::OK();
}

Status StructReader::GetRepLevels(const int16_t** data, int64_t* length) {
  *data = nullptr;
  if (children_.size() == 0) {
    *length = 0;
    return Status::Invalid("StructReader had no childre");
  }

  // This method should only be called when this struct or one of its parents
  // are optional/repeated or it has repeated child.
  // Meaning all children must have rep/def levels associated
  // with them.
  RETURN_NOT_OK(def_rep_level_child_->GetRepLevels(data, length));

  if (data == nullptr) {
    // Only happens if there are actually 0 rows available.
    *data = nullptr;
    *length = 0;
  }
  return Status::OK();
}

Status StructReader::BuildArray(int64_t records_to_read,
                                std::shared_ptr<ChunkedArray>* out) {
  std::vector<std::shared_ptr<ArrayData>> children_array_data;
  std::shared_ptr<Buffer> null_bitmap;

  ::parquet::internal::ValidityBitmapInputOutput validity_io;
  validity_io.values_read_upper_bound = records_to_read;
  validity_io.values_read = records_to_read;

  BEGIN_PARQUET_CATCH_EXCEPTIONS
  const int16_t* def_levels;
  const int16_t* rep_levels;
  int64_t num_levels;

  if (has_repeated_child_) {
    ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(records_to_read, ctx_->pool));
    validity_io.valid_bits = null_bitmap->mutable_data();
    RETURN_NOT_OK(GetDefLevels(&def_levels, &num_levels));
    RETURN_NOT_OK(GetRepLevels(&rep_levels, &num_levels));
    DefRepLevelsToBitmap(def_levels, rep_levels, num_levels, level_info_, &validity_io);
  } else if (filtered_field_->nullable()) {
    ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(records_to_read, ctx_->pool));
    validity_io.valid_bits = null_bitmap->mutable_data();
    RETURN_NOT_OK(GetDefLevels(&def_levels, &num_levels));
    DefLevelsToBitmap(def_levels, num_levels, level_info_, &validity_io);
  }

  // Ensure all values are initialized.
  if (null_bitmap && BitUtil::BytesForBits(validity_io.values_read) <
                         BitUtil::BytesForBits(records_to_read)) {
    std::fill(
        null_bitmap->mutable_data() + BitUtil::BytesForBits(validity_io.values_read),
        null_bitmap->mutable_data() + BitUtil::BytesForBits(records_to_read), 0);
  }

  END_PARQUET_CATCH_EXCEPTIONS
  // Gather children arrays and def levels
  for (auto& child : children_) {
    std::shared_ptr<ChunkedArray> field;
    RETURN_NOT_OK(child->BuildArray(validity_io.values_read, &field));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> array_data, ChunksToSingle(*field));
    children_array_data.push_back(std::move(array_data));
  }

  if (!filtered_field_->nullable() && !has_repeated_child_) {
    validity_io.values_read = children_array_data.front()->length;
  }

  std::vector<std::shared_ptr<Buffer>> buffers{
      validity_io.null_count > 0 ? null_bitmap : std::shared_ptr<Buffer>()};
  auto data =
      std::make_shared<ArrayData>(filtered_field_->type(),
                                  /*length=*/validity_io.values_read, std::move(buffers),
                                  std::move(children_array_data));
  std::shared_ptr<Array> result = ::arrow::MakeArray(data);
  RETURN_NOT_OK(result->Validate());

  *out = std::make_shared<ChunkedArray>(result);
  return Status::OK();
}

// ----------------------------------------------------------------------
// File reader implementation

Status GetReader(const SchemaField& field, const std::shared_ptr<ReaderContext>& ctx,
                 std::unique_ptr<ColumnReaderImpl>* out) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS

  auto type_id = field.field->type()->id();
  if (field.children.size() == 0) {
    if (!field.is_leaf()) {
      return Status::Invalid("Parquet non-leaf node has no children");
    }
    if (!ctx->IncludesLeaf(field.column_index)) {
      *out = nullptr;
      return Status::OK();
    }
    std::unique_ptr<FileColumnIterator> input(
        ctx->iterator_factory(field.column_index, ctx->reader));
    out->reset(new LeafReader(ctx, field.field, std::move(input), field.level_info));
  } else if (type_id == ::arrow::Type::LIST) {
    auto list_field = field.field;
    auto child = &field.children[0];
    std::unique_ptr<ColumnReaderImpl> child_reader;
    RETURN_NOT_OK(GetReader(*child, ctx, &child_reader));
    if (child_reader == nullptr) {
      *out = nullptr;
      return Status::OK();
    }
    out->reset(new ListReader<int32_t>(ctx, list_field, field.level_info,
                                       std::move(child_reader)));
  } else if (type_id == ::arrow::Type::STRUCT) {
    std::vector<std::shared_ptr<Field>> child_fields;
    std::vector<std::unique_ptr<ColumnReaderImpl>> child_readers;
    for (const auto& child : field.children) {
      std::unique_ptr<ColumnReaderImpl> child_reader;
      RETURN_NOT_OK(GetReader(child, ctx, &child_reader));
      if (!child_reader) {
        // If all children were pruned, then we do not try to read this field
        continue;
      }
      child_fields.push_back(child.field);
      child_readers.emplace_back(std::move(child_reader));
    }
    if (child_fields.size() == 0) {
      *out = nullptr;
      return Status::OK();
    }
    auto filtered_field =
        ::arrow::field(field.field->name(), ::arrow::struct_(child_fields),
                       field.field->nullable(), field.field->metadata());
    out->reset(new StructReader(ctx, filtered_field, field.level_info,
                                std::move(child_readers)));
  } else {
    return Status::Invalid("Unsupported nested type: ", field.field->ToString());
  }
  return Status::OK();

  END_PARQUET_CATCH_EXCEPTIONS
}

Status FileReaderImpl::GetRecordBatchReader(const std::vector<int>& row_groups,
                                            const std::vector<int>& column_indices,
                                            std::unique_ptr<RecordBatchReader>* out) {
  RETURN_NOT_OK(BoundsCheck(row_groups, column_indices));

  if (reader_properties_.pre_buffer()) {
    // PARQUET-1698/PARQUET-1820: pre-buffer row groups/column chunks if enabled
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    reader_->PreBuffer(row_groups, column_indices, reader_properties_.async_context(),
                       reader_properties_.cache_options());
    END_PARQUET_CATCH_EXCEPTIONS
  }

  std::vector<std::shared_ptr<ColumnReaderImpl>> readers;
  std::shared_ptr<::arrow::Schema> batch_schema;
  RETURN_NOT_OK(GetFieldReaders(column_indices, row_groups, &readers, &batch_schema));

  if (readers.empty()) {
    // Just generate all batches right now; they're cheap since they have no columns.
    int64_t batch_size = properties().batch_size();
    auto max_sized_batch =
        ::arrow::RecordBatch::Make(batch_schema, batch_size, ::arrow::ArrayVector{});

    ::arrow::RecordBatchVector batches;

    for (int row_group : row_groups) {
      int64_t num_rows = parquet_reader()->metadata()->RowGroup(row_group)->num_rows();

      batches.insert(batches.end(), num_rows / batch_size, max_sized_batch);

      if (int64_t trailing_rows = num_rows % batch_size) {
        batches.push_back(max_sized_batch->Slice(0, trailing_rows));
      }
    }

    *out = ::arrow::internal::make_unique<RowGroupRecordBatchReader>(
        ::arrow::MakeVectorIterator(std::move(batches)), std::move(batch_schema));

    return Status::OK();
  }

  using ::arrow::RecordBatchIterator;

  // NB: This lambda will be invoked outside the scope of this call to
  // `GetRecordBatchReader()`, so it must capture `readers` and `batch_schema` by value.
  // `this` is a non-owning pointer so we are relying on the parent FileReader outliving
  // this RecordBatchReader.
  ::arrow::Iterator<RecordBatchIterator> batches = ::arrow::MakeFunctionIterator(
      [readers, batch_schema, this]() -> ::arrow::Result<RecordBatchIterator> {
        ::arrow::ChunkedArrayVector columns(readers.size());
        for (size_t i = 0; i < columns.size(); ++i) {
          RETURN_NOT_OK(readers[i]->NextBatch(properties().batch_size(), &columns[i]));
          if (columns[i] == nullptr || columns[i]->length() == 0) {
            return ::arrow::IterationTraits<RecordBatchIterator>::End();
          }
        }

        auto table = ::arrow::Table::Make(batch_schema, std::move(columns));
        auto table_reader = std::make_shared<::arrow::TableBatchReader>(*table);

        // NB: explicitly preserve table so that table_reader doesn't outlive it
        return ::arrow::MakeFunctionIterator(
            [table, table_reader] { return table_reader->Next(); });
      });

  *out = ::arrow::internal::make_unique<RowGroupRecordBatchReader>(
      ::arrow::MakeFlattenIterator(std::move(batches)), std::move(batch_schema));

  return Status::OK();
}

Status FileReaderImpl::GetColumn(int i, FileColumnIteratorFactory iterator_factory,
                                 std::unique_ptr<ColumnReader>* out) {
  RETURN_NOT_OK(BoundsCheckColumn(i));
  auto ctx = std::make_shared<ReaderContext>();
  ctx->reader = reader_.get();
  ctx->pool = pool_;
  ctx->iterator_factory = iterator_factory;
  ctx->filter_leaves = false;
  std::unique_ptr<ColumnReaderImpl> result;
  RETURN_NOT_OK(GetReader(manifest_.schema_fields[i], ctx, &result));
  out->reset(result.release());
  return Status::OK();
}

Status FileReaderImpl::ReadRowGroups(const std::vector<int>& row_groups,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<Table>* out) {
  RETURN_NOT_OK(BoundsCheck(row_groups, column_indices));

  // PARQUET-1698/PARQUET-1820: pre-buffer row groups/column chunks if enabled
  if (reader_properties_.pre_buffer()) {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    parquet_reader()->PreBuffer(row_groups, column_indices,
                                reader_properties_.async_context(),
                                reader_properties_.cache_options());
    END_PARQUET_CATCH_EXCEPTIONS
  }

  std::vector<std::shared_ptr<ColumnReaderImpl>> readers;
  std::shared_ptr<::arrow::Schema> result_schema;
  RETURN_NOT_OK(GetFieldReaders(column_indices, row_groups, &readers, &result_schema));

  ::arrow::ChunkedArrayVector columns(readers.size());
  auto ReadColumnFunc = [&](size_t i) {
    return ReadColumn(static_cast<int>(i), row_groups, readers[i].get(), &columns[i]);
  };

  if (reader_properties_.use_threads()) {
    std::vector<Future<Status>> futures(readers.size());
    auto pool = ::arrow::internal::GetCpuThreadPool();
    for (size_t i = 0; i < readers.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(futures[i], pool->Submit(ReadColumnFunc, i));
    }

    Status final_status;
    for (auto& fut : futures) {
      final_status &= fut.status();
    }
    RETURN_NOT_OK(final_status);
  } else {
    for (size_t i = 0; i < readers.size(); ++i) {
      RETURN_NOT_OK(ReadColumnFunc(i));
    }
  }

  int64_t num_rows = 0;
  if (!columns.empty()) {
    num_rows = columns[0]->length();
  } else {
    for (int i : row_groups) {
      num_rows += parquet_reader()->metadata()->RowGroup(i)->num_rows();
    }
  }

  *out = Table::Make(std::move(result_schema), std::move(columns), num_rows);
  return (*out)->Validate();
}

std::shared_ptr<RowGroupReader> FileReaderImpl::RowGroup(int row_group_index) {
  return std::make_shared<RowGroupReaderImpl>(this, row_group_index);
}

// ----------------------------------------------------------------------
// Public factory functions

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<RecordBatchReader> tmp;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(row_group_indices, &tmp));
  out->reset(tmp.release());
  return Status::OK();
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        const std::vector<int>& column_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<RecordBatchReader> tmp;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(row_group_indices, column_indices, &tmp));
  out->reset(tmp.release());
  return Status::OK();
}

Status FileReader::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileReader> reader,
                        const ArrowReaderProperties& properties,
                        std::unique_ptr<FileReader>* out) {
  out->reset(new FileReaderImpl(pool, std::move(reader), properties));
  return static_cast<FileReaderImpl*>(out->get())->Init();
}

Status FileReader::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileReader> reader,
                        std::unique_ptr<FileReader>* out) {
  return Make(pool, std::move(reader), default_arrow_reader_properties(), out);
}

FileReaderBuilder::FileReaderBuilder()
    : pool_(::arrow::default_memory_pool()),
      properties_(default_arrow_reader_properties()) {}

Status FileReaderBuilder::Open(std::shared_ptr<::arrow::io::RandomAccessFile> file,
                               const ReaderProperties& properties,
                               std::shared_ptr<FileMetaData> metadata) {
  PARQUET_CATCH_NOT_OK(raw_reader_ = ParquetReader::Open(std::move(file), properties,
                                                         std::move(metadata)));
  return Status::OK();
}

FileReaderBuilder* FileReaderBuilder::memory_pool(::arrow::MemoryPool* pool) {
  pool_ = pool;
  return this;
}

FileReaderBuilder* FileReaderBuilder::properties(
    const ArrowReaderProperties& arg_properties) {
  properties_ = arg_properties;
  return this;
}

Status FileReaderBuilder::Build(std::unique_ptr<FileReader>* out) {
  return FileReader::Make(pool_, std::move(raw_reader_), properties_, out);
}

Status OpenFile(std::shared_ptr<::arrow::io::RandomAccessFile> file, MemoryPool* pool,
                std::unique_ptr<FileReader>* reader) {
  FileReaderBuilder builder;
  RETURN_NOT_OK(builder.Open(std::move(file)));
  return builder.memory_pool(pool)->Build(reader);
}

namespace internal {

Status FuzzReader(std::unique_ptr<FileReader> reader) {
  auto st = Status::OK();
  for (int i = 0; i < reader->num_row_groups(); ++i) {
    std::shared_ptr<Table> table;
    auto row_group_status = reader->ReadRowGroup(i, &table);
    if (row_group_status.ok()) {
      row_group_status &= table->ValidateFull();
    }
    st &= row_group_status;
  }
  return st;
}

Status FuzzReader(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<::arrow::Buffer>(data, size);
  auto file = std::make_shared<::arrow::io::BufferReader>(buffer);
  FileReaderBuilder builder;
  RETURN_NOT_OK(builder.Open(std::move(file)));

  std::unique_ptr<FileReader> reader;
  RETURN_NOT_OK(builder.Build(&reader));
  return FuzzReader(std::move(reader));
}

}  // namespace internal

}  // namespace arrow
}  // namespace parquet
