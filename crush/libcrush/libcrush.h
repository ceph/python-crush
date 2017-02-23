#ifndef _LIBCRUSH_H
#define _LIBCRUSH_H

#include <Python.h>
#include <structmember.h>
#include <opcode.h>
#include <frameobject.h>

#if PY_MAJOR_VERSION >= 3

#define MyText_Type                     PyUnicode_Type
#define MyText_AS_BYTES(o)              PyUnicode_AsASCIIString(o)
#define MyBytes_GET_SIZE(o)             PyBytes_GET_SIZE(o)
#define MyBytes_AS_STRING(o)            PyBytes_AS_STRING(o)
#define MyText_AsString(o)              PyUnicode_AsUTF8(o)
#define MyText_FromFormat               PyUnicode_FromFormat
#define MyInt_FromInt(i)                PyLong_FromLong((long)i)
#define MyInt_AsInt(o)                  (int)PyLong_AsLong(o)
#define MyText_InternFromString(s)      PyUnicode_InternFromString(s)

#define MyType_HEAD_INIT                PyVarObject_HEAD_INIT(NULL, 0)

#else

#define MyText_Type                     PyString_Type
#define MyText_AS_BYTES(o)              (Py_INCREF(o), o)
#define MyBytes_GET_SIZE(o)             PyString_GET_SIZE(o)
#define MyBytes_AS_STRING(o)            PyString_AS_STRING(o)
#define MyText_AsString(o)              PyString_AsString(o)
#define MyText_FromFormat               PyUnicode_FromFormat
#define MyInt_FromInt(i)                PyInt_FromLong((long)i)
#define MyInt_AsInt(o)                  (int)PyInt_AsLong(o)
#define MyText_InternFromString(s)      PyString_InternFromString(s)

#define MyType_HEAD_INIT                PyObject_HEAD_INIT(NULL)  0,

#endif /* Py3k */

/* The LibCrush type. */

#include "crush.h"

typedef struct LibCrush {
  PyObject_HEAD

  int verbose;
  int has_bucket_weights;
  struct crush_map *map;
  PyObject *types;
  PyObject *items;
  PyObject *ritems;
  int highest_device_id;
  PyObject *rules;
} LibCrush;

extern PyTypeObject LibCrushType;

#endif /* _LIBCRUSH_H */
