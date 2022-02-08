#include "arrow/python/arrow_to_python.h"

namespace arrow {
namespace py {
namespace {

// Wrapper around a Python list object that mimics dereference and assignment
// operations.
struct PyListAssigner {
 public:
  explicit PyListAssigner(PyObject* list) : list_(list) { DCHECK(PyList_Check(list_)); }

  PyListAssigner& operator*() { return *this; }

  void operator=(PyObject* obj) {
    if (ARROW_PREDICT_FALSE(PyList_SetItem(list_, current_index_, obj) == -1)) {
      Py_FatalError("list did not have the correct preallocated size.");
    }
  }

  PyListAssigner& operator++() {
    current_index_++;
    return *this;
  }

  PyListAssigner& operator+=(int64_t offset) {
    current_index_ += offset;
    return *this;
  }

 private:
  PyObject* list_;
  int64_t current_index_ = 0;
};

struct IndexRange {
  // 
  int64_t start;
  // End (Exclusive).
  int64_t end;

  bool Contains(in64_t index) {
    return index >= start && index < end;
  }
}

// Wrapper around a Python list of list object that mimics dereference and assignment
// operations. This assigner is needed for ListArrays where the expected output
// is a list of lists (e.g. [[1, 2], None, [4,5,6]])
struct PyNestedListAssigner {
 public:
  explicit PyListAssigner(PyObject* outer_list, std::vector<IndexRange> null_ranges) : outer_list_(outer_list), 
     null_ranges_(std::move(null_ranges)), current_null_range_(null_ranges_.begin()) 
  { DCHECK(PyList_Check(list_)); 
    if (PyList_GET_SIZE(list_) > 0) {
      // Advance to first assignable sublist.
      (*self)++;
    }
  }

  PyListAssigner& operator*() { return *this; }

  void operator=(PyObject* obj) {
    if (ARROW_PREDICT_FALSE(current_null_range_ != null_ranges_.end())) {
      if (current_null_range_->Contains(current_index_)) {
        Py_DECREF(obj);
        // This indicates that index of the value passed in corresponds
        // to a non-empty list that is "hidden" but a is_valid = False
        // in the parent array.
        // e.g. The List Array looks like:
        //    valid = [True, False, True]
        //    offsets = [0, 1, 2, 3]
        // An alterative way to handle this would be collapse the values array
        // but this case is likely uncommon in practice so the cost of
        // attemping to collapse are ListArray values is move expensive
        // then allowing conversion (this hasn't been validated in benchmarks
        // so if there are reports of slowness in handling this case we
        // can reconsider).
        return;
      }
    }
    int64_t current_index_offset = current_index_ - current_sublist_offset_;
    if (ARROW_PREDICT_FALSE(PyList_SetItem(current_list(), current_list_index, obj) == -1)) {
      Py_DECREF(obj);
      Py_FatalError("list did not have the correct preallocated size.");
    }
  }

  PyNestedListAssigner& operator++() {
    current_index_++;
    if (ARROW_PREDICT_FALSE(current_null_range_ != null_ranges_.end() &&
        current_index_ >= current_null_range_->end)) {
      sub_list_offset_ +=  current_null_range_->Range();
      ++current_null_range_;
    }
    int64_t current_sublist_offset_ = current_index_ - sub_list_offset_;
    // Cases of current list AND 0-length lists (None lists are handled in
    // current_list_size). 
    while (current_sublist_offset_ == current_list_size() 
           && current_sublist_index_ < PyList_GET_SIZE(parent_list_)) {
      sub_list_offset_ += current_list_size(); 
      current_sublist_index_++;
    }
     return *this;
  }
  // TODO: current_list_size needs to account for None
  
 private:
  PyObject* parent_list_;
  int64_t current_index_ = -1;
  int64_t sub_list_offset_ = 0;
  int64_t current_sublist_index_ = 0;
  const std::vector<Range> null_ranges_;
  std::vector<Range>::iterator current_null_range_;
};


// Wrapper around a Python list of dict object that mimics dereference and assignment
// operations. This assigner is needed for StructArrays where the expected output
// is a list of dicts (e.g. [{a: 1, b: 2}, None]]).  This Assigner is designed
// to fill one key for each element in a list in sequence.  Callers will
// create one for each key as necessary.
struct PyNestedDictAssigner {
 public:
  explicit PyNestedDictAssigner(ListAssigner(PyObject* outer_list, PyObject* keys_list) : outer_list_(outer_list) { 
    DCHECK(PyList_Check(outer_list_)); 
  }


  PyNestedDictAssigner& operator*() { return *this; }

  void operator=(PyObject* obj) {
    PyObject* maybe_dict = PyList_GET_ITEM(outer_list_, current_index_);
    if (maybe_dict != PyNone) {
      DCHECK(PyDict_Check(maybe_dict));
      if (PyDict_SetItem(maybe_dict, current_key_, obj) == -1) {
        PyFatalError("Error setting value in dict");
      }
    }
    Py_DECREF(obj);
  }

  PyNestedListAssigner& operator++() {
    current_index_++;
    return *this;
  }

 private:
  PyObject* outer_list_;
  int64_t current_index_ = -1;
};


// Converter for Primitive (non-nested types) to there corresponding
// Python types. Nested types
template<typename ContainerSetter>
PrimitiveValueDataVisitor {

  ContainerSetter& setter;
  Status maybe_error;
};

template<typename ContainerSetter>
template TopLevelArrayVisitor {

  template <typename T>
  ::arrow::enable_if_t<std::is_base_of<::arrow::FlatArray, T>::value, Status> Visit(
      const T& array) {
    AddTerminalInfo(array);
    return Status::OK();
  }

  template <typename T>
  ::arrow::enable_if_t<std::is_same<::arrow::ListArray, T>::value ||
                           std::is_same<::arrow::LargeListArray, T>::value,
                       Status>
  Visit(const T& array) {
  }

Status Visit(const ::arrow::FixedSizeListArray& array) {
}

  Status Visit(const ::arrow::ExtensionArray& array) {
    return VisitInline(*array.storage());
  }

 Status Visit(const ::arrow::ExtensionArray& array) {
    return VisitInline(*array.storage());
  }

 Status Visit(const ::arrow::UnionArray& array) {
    OwnedRef ret_list = PyList_New(array.length); 
    if (ret_list.obj() == nullptr) {
      RETURN_IF_PYERROR();
    }
    for (int x = 0; x < array.length(); x++) {
      ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar, array.GetScalar(x));
      // This is going to be pretty inefficient, but union's are pretty
      // uncommon so we can wait to see if optimization is necessary.
      if (scalar->is_valid) {
        ASSIGN_OR_RAISE(PyObject* py_obj, ToPyObject(scalar->value));
        PyList_SET_ITEM(ret_list.obj(), x, py_obj);
      } else {
        Py_INCREF(PyNone);
        PyList_SET_ITEM(ret_list.obj(), x, PyNone);
      }
    }
  }

Status Visit(const ::arrow::StructArray& array) {
  std::shared_ptr<Buffer> null_bitmap = array.null_bitmap();
  int num_fields = array.type()->num_fields();
  OwnedRef field_names = PyList_New(nume_fields);
  OwnedRef ret_list = PyList_New(array.length());
  OwnedRef dict_result = PyDict_New();
  for (int x = 0; x < num_fields; x++) {
    if (PyDict_SetItem(dict_result, PyDict_GET_ITEM(field_names.obj(), x),
                       PyNone) == -1) {
      Py_FatalError("Couldn't populate struct names");
    }
  }
  if (PyDict_Size(dict_result.obj()) != num_fields) {
    return Status::InvalidError("Duplicate field names in struct:", PyObect_StdStringRepr(field_names.obj()));
  }

  // TODO: try to avoid one extra copy?
  if (null_bitmap != nullptr && array.null_count() > 0) {
    std::shared_ptr<BooleanArray> valid_values = IsValid(...)
  } else {
    for (int x = 0; x < array.length(); x++) {
      PyObject* result_copy = PyDict_Copy(dict_result.obj());
      PyList_Set
    }
  }

  ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Array>> arrays,
                  array.Flatten());

}


  ContainerSetter& setter;

};



Result<std::shared_ptr<Array>> ScalarToArray(const Scalar& scalar, MemoryPool* pool) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(pool, scalar.type(), &out));
  RETURN_NOT_OK(builder->AppendScalar(scalar));
  return builder->Finish();
}




cdef class NullScalar(Scalar):
    def as_py(self):
        """
        Return this value as a Python None.
        """
        return None


cdef class BooleanScalar(Scalar):
    """
    Concrete class for boolean scalars.
    """

    def as_py(self):
        """
        Return this value as a Python bool.
        """
        cdef CBooleanScalar* sp = <CBooleanScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class UInt8Scalar(Scalar):
    """
    Concrete class for uint8 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CUInt8Scalar* sp = <CUInt8Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None

cdef class HalfFloatScalar(Scalar):
    """
    Concrete class for float scalars.
    """

    def as_py(self):
        """
        Return this value as a Python float.
        """
        cdef CHalfFloatScalar* sp = <CHalfFloatScalar*> self.wrapped.get()
        return PyHalf_FromHalf(sp.value) if sp.is_valid else None


cdef class FloatScalar(Scalar):
    """
    Concrete class for float scalars.
    """

    def as_py(self):
        """
        Return this value as a Python float.
        """
        cdef CFloatScalar* sp = <CFloatScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None

cdef class Decimal128Scalar(Scalar):
cdef class Decimal256Scalar(Scalar):
    """
    Concrete class for decimal128 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python Decimal.
        """
        cdef:
            CDecimal128Scalar* sp = <CDecimal128Scalar*> self.wrapped.get()
            CDecimal128Type* dtype = <CDecimal128Type*> sp.type.get()
        if sp.is_valid:
            return _pydecimal.Decimal(
                frombytes(sp.value.ToString(dtype.scale()))
            )
        else:
            return None



cdef class Date32Scalar(Scalar):
    """
    Concrete class for date32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.datetime instance.
        """
        cdef CDate32Scalar* sp = <CDate32Scalar*> self.wrapped.get()

        if sp.is_valid:
            # shift to seconds since epoch
            return (
                datetime.date(1970, 1, 1) + datetime.timedelta(days=sp.value)
            )
        else:
            return None


cdef class Date64Scalar(Scalar):
    """
    Concrete class for date64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.datetime instance.
        """
        cdef CDate64Scalar* sp = <CDate64Scalar*> self.wrapped.get()

        if sp.is_valid:
            return (
                datetime.date(1970, 1, 1) +
                datetime.timedelta(days=sp.value / 86400000)
            )
        else:
            return None


def _datetime_from_int(int64_t value, TimeUnit unit, tzinfo=None):
    if unit == TimeUnit_SECOND:
        delta = datetime.timedelta(seconds=value)
    elif unit == TimeUnit_MILLI:
        delta = datetime.timedelta(milliseconds=value)
    elif unit == TimeUnit_MICRO:
        delta = datetime.timedelta(microseconds=value)
    else:
        # TimeUnit_NANO: prefer pandas timestamps if available
        if _pandas_api.have_pandas:
            return _pandas_api.pd.Timestamp(value, tz=tzinfo, unit='ns')
        # otherwise safely truncate to microsecond resolution datetime
        if value % 1000 != 0:
            raise ValueError(
                "Nanosecond resolution temporal type {} is not safely "
                "convertible to microseconds to convert to datetime.datetime. "
                "Install pandas to return as Timestamp with nanosecond "
                "support or access the .value attribute.".format(value)
            )
        delta = datetime.timedelta(microseconds=value // 1000)

    dt = datetime.datetime(1970, 1, 1) + delta
    # adjust timezone if set to the datatype
    if tzinfo is not None:
        dt = tzinfo.fromutc(dt)

    return dt


cdef class Time32Scalar(Scalar):
    """
    Concrete class for time32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.timedelta instance.
        """
        cdef:
            CTime32Scalar* sp = <CTime32Scalar*> self.wrapped.get()
            CTime32Type* dtype = <CTime32Type*> sp.type.get()

        if sp.is_valid:
            return _datetime_from_int(sp.value, unit=dtype.unit()).time()
        else:
            return None


cdef class Time64Scalar(Scalar):
    """
    Concrete class for time64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.timedelta instance.
        """
        cdef:
            CTime64Scalar* sp = <CTime64Scalar*> self.wrapped.get()
            CTime64Type* dtype = <CTime64Type*> sp.type.get()

        if sp.is_valid:
            return _datetime_from_int(sp.value, unit=dtype.unit()).time()
        else:
            return None


cdef class TimestampScalar(Scalar):
    """
    Concrete class for timestamp scalars.
    """

    def as_py(self):
        """
        Return this value as a Pandas Timestamp instance (if units are
        nanoseconds and pandas is available), otherwise as a Python
        datetime.datetime instance.
        """
        cdef:
            CTimestampScalar* sp = <CTimestampScalar*> self.wrapped.get()
            CTimestampType* dtype = <CTimestampType*> sp.type.get()

        if not sp.is_valid:
            return None

        if not dtype.timezone().empty():
            tzinfo = string_to_tzinfo(frombytes(dtype.timezone()))
        else:
            tzinfo = None

        return _datetime_from_int(sp.value, unit=dtype.unit(), tzinfo=tzinfo)


cdef class DurationScalar(Scalar):
    """
    Concrete class for duration scalars.
    """

    @property
    def value(self):
        cdef CDurationScalar* sp = <CDurationScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None

    def as_py(self):
        """
        Return this value as a Pandas Timedelta instance (if units are
        nanoseconds and pandas is available), otherwise as a Python
        datetime.timedelta instance.
        """
        cdef:
            CDurationScalar* sp = <CDurationScalar*> self.wrapped.get()
            CDurationType* dtype = <CDurationType*> sp.type.get()
            TimeUnit unit = dtype.unit()

        if not sp.is_valid:
            return None

        if unit == TimeUnit_SECOND:
            return datetime.timedelta(seconds=sp.value)
        elif unit == TimeUnit_MILLI:
            return datetime.timedelta(milliseconds=sp.value)
        elif unit == TimeUnit_MICRO:
            return datetime.timedelta(microseconds=sp.value)
        else:
            # TimeUnit_NANO: prefer pandas timestamps if available
            if _pandas_api.have_pandas:
                return _pandas_api.pd.Timedelta(sp.value, unit='ns')
            # otherwise safely truncate to microsecond resolution timedelta
            if sp.value % 1000 != 0:
                raise ValueError(
                    "Nanosecond duration {} is not safely convertible to "
                    "microseconds to convert to datetime.timedelta. Install "
                    "pandas to return as Timedelta with nanosecond support or "
                    "access the .value attribute.".format(sp.value)
                )
            return datetime.timedelta(microseconds=sp.value // 1000)


cdef class MonthDayNanoIntervalScalar(Scalar):
    """
    Concrete class for month, day, nanosecond interval scalars.
    """

    @property
    def value(self):
        """
        Same as self.as_py()
        """
        return self.as_py()

    def as_py(self):
        """
        Return this value as a pyarrow.MonthDayNano.
        """
        cdef:
            PyObject* val
            CMonthDayNanoIntervalScalar* scalar
        scalar = <CMonthDayNanoIntervalScalar*>self.wrapped.get()
        val = GetResultValue(MonthDayNanoIntervalScalarToPyObject(
            deref(scalar)))
        return PyObject_to_object(val)


cdef class BinaryScalar(Scalar):
    """
    Concrete class for binary-like scalars.
    """
    def as_py(self):
        """
        Return this value as a Python bytes.
        """
        buffer = self.as_buffer()
        return None if buffer is None else buffer.to_pybytes()


cdef class LargeBinaryScalar(BinaryScalar):
    pass


cdef class FixedSizeBinaryScalar(BinaryScalar):
    pass


cdef class StringScalar(BinaryScalar):
    """
    Concrete class for string-like (utf8) scalars.
    """

    def as_py(self):
        """
        Return this value as a Python string.
        """
        buffer = self.as_buffer()
        return None if buffer is None else str(buffer, 'utf8')


cdef class FixedSizeListScalar(ListScalar):
cdef class LargeListScalar(ListScalar):
cdef class ListScalar(Scalar):
    """
    Concrete class for list-like scalars.
    """

    @property
    def values(self):
        cdef CBaseListScalar* sp = <CBaseListScalar*> self.wrapped.get()
        if sp.is_valid:
            return pyarrow_wrap_array(sp.value)
        else:
            return None

    def as_py(self):
        """
        Return this value as a Python list.
        """
        arr = self.values
        return None if arr is None else arr.to_pylist()



cdef class StructScalar(Scalar, collections.abc.Mapping):
    """
    Concrete class for struct scalars.
    """

    def __len__(self):
        cdef CStructScalar* sp = <CStructScalar*> self.wrapped.get()
        return sp.value.size()

    def __iter__(self):
        cdef:
            CStructScalar* sp = <CStructScalar*> self.wrapped.get()
            CStructType* dtype = <CStructType*> sp.type.get()
            vector[shared_ptr[CField]] fields = dtype.fields()

        for i in range(dtype.num_fields()):
            yield frombytes(fields[i].get().name())

    def items(self):
        return ((key, self[i]) for i, key in enumerate(self))

    def __contains__(self, key):
        return key in list(self)

    def __getitem__(self, key):
        """
        Return the child value for the given field.

        Parameters
        ----------
        index : Union[int, str]
            Index / position or name of the field.

        Returns
        -------
        result : Scalar
        """
        cdef:
            CFieldRef ref
            CStructScalar* sp = <CStructScalar*> self.wrapped.get()

        if isinstance(key, (bytes, str)):
            ref = CFieldRef(<c_string> tobytes(key))
        elif isinstance(key, int):
            ref = CFieldRef(<int> key)
        else:
            raise TypeError('Expected integer or string index')

        try:
            return Scalar.wrap(GetResultValue(sp.field(ref)))
        except ArrowInvalid as exc:
            if isinstance(key, int):
                raise IndexError(key) from exc
            else:
                raise KeyError(key) from exc

    def as_py(self):
        """
        Return this value as a Python dict.
        """
        if self.is_valid:
            try:
                return {k: self[k].as_py() for k in self.keys()}
            except KeyError:
                raise ValueError(
                    "Converting to Python dictionary is not supported when "
                    "duplicate field names are present")
        else:
            return None

    def _as_py_tuple(self):
        # a version that returns a tuple instead of dict to support repr/str
        # with the presence of duplicate field names
        if self.is_valid:
            return [(key, self[i].as_py()) for i, key in enumerate(self)]
        else:
            return None

cdef class MapScalar(ListScalar):
    """
    Concrete class for map scalars.
    """

    def __iter__(self):
        """
        Iterate over this element's values.
        """
        arr = self.values
        if array is None:
            raise StopIteration
        for k, v in zip(arr.field('key'), arr.field('value')):
            yield (k.as_py(), v.as_py())

    def as_py(self):
        """
        Return this value as a Python list.
        """
        cdef CStructScalar* sp = <CStructScalar*> self.wrapped.get()
        return list(self) if sp.is_valid else None


cdef class DictionaryScalar(Scalar):
    """
    Concrete class for dictionary-encoded scalars.
    """

    @property
    def value(self):
        """
        Return the encoded value as a scalar.
        """
        cdef CDictionaryScalar* sp = <CDictionaryScalar*> self.wrapped.get()
        return Scalar.wrap(GetResultValue(sp.GetEncodedValue()))

    @property
    def dictionary(self):
        cdef CDictionaryScalar* sp = <CDictionaryScalar*> self.wrapped.get()
        return pyarrow_wrap_array(sp.value.dictionary)

    def as_py(self):
        """
        Return this encoded value as a Python object.
        """
        return self.value.as_py() if self.is_valid else None


cdef class UnionScalar(Scalar):
    """
    Concrete class for Union scalars.
    """

    @property
    def value(self):
        """
        Return underlying value as a scalar.
        """
        cdef CUnionScalar* sp = <CUnionScalar*> self.wrapped.get()
        return Scalar.wrap(sp.value) if sp.is_valid else None

    def as_py(self):
        """
        Return underlying value as a Python object.
        """
        value = self.value
        return None if value is None else value.as_py()


cdef class ExtensionScalar(Scalar):
    """
    Concrete class for Extension scalars.
    """

    @property
    def value(self):
        """
        Return storage value as a scalar.
        """
        cdef CExtensionScalar* sp = <CExtensionScalar*> self.wrapped.get()
        return Scalar.wrap(sp.value) if sp.is_valid else None

    def as_py(self):
        """
        Return this scalar as a Python object.
        """
        # XXX should there be a hook to wrap the result in a custom class?
        value = self.value
        return None if value is None else value.as_py()

}

Result<PyObject*> ArrowToPythonConverter::ToPyList(const Array& array) {

}

Result<PyObject*> ArrowToPythonConverter::ToPyObject(const Scalar& scalar) {
  // Converting to Array first has performance overhead but this method
  // shouldn't be on the performance critical path.
  std::shard_ptr<Array> array;
  ASSIGN_OR_RAISE(array, ScalarToArray(scalar));
  OwnedObject list;
  ASSIGN_OR_RAISE(list, ToPyList(array));
  PyObject* ret = PyList_GET_ITEM(list.obj(), 0);
  // GetItem returns a borrowed reference.
  Py_INCREF(ret);
  return ret;
}


} // namespace py
} // namespace arrow

