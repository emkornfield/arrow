#ifndef LOCAL_GOOGLE_HOME_MICAHK_ARROW_ARROW_CPP_SRC_ARROW_PYTHON_ARROW_TO_PYTHON_H_
#define LOCAL_GOOGLE_HOME_MICAHK_ARROW_ARROW_CPP_SRC_ARROW_PYTHON_ARROW_TO_PYTHON_H_

namespace arrow {
namespace py {

class ArrowToPythonConverter {
 public:
  PyObject* ToPyList(const Array& array);
  PyObject* ToPyObject(const Scalar& scalar);
 private:
  OwnedObject decimal_;
};

} // py
} / arrow


#endif  // LOCAL_GOOGLE_HOME_MICAHK_ARROW_ARROW_CPP_SRC_ARROW_PYTHON_ARROW_TO_PYTHON_H_
