
#include "libcrush.h"

#define RET_OK      0
#define RET_ERROR   -1

static int
LibCrush_init(LibCrush *self, PyObject *args_unused, PyObject *kwds_unused)
{
    int ret = RET_ERROR;

    ret = RET_OK;

    return ret;
}

static void
LibCrush_dealloc(LibCrush *self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMemberDef
LibCrush_members[] = {
    { NULL }
};

static PyMethodDef
LibCrush_methods[] = {
    { NULL }
};

PyTypeObject
LibCrushType = {
    MyType_HEAD_INIT
    "crush.LibCrush",          /*tp_name*/
    sizeof(LibCrush),          /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)LibCrush_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "LibCrush objects",         /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    LibCrush_methods,          /* tp_methods */
    LibCrush_members,          /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)LibCrush_init,   /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};
