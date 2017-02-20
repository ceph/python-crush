#include "libcrush.h"

/* Module definition */

#define MODULE_DOC PyDoc_STR("python wrapper for libcrush.")

#if PY_MAJOR_VERSION >= 3

static PyModuleDef
moduledef = {
    PyModuleDef_HEAD_INIT,
    "crush.libcrush",
    MODULE_DOC,
    -1,
    NULL,       /* methods */
    NULL,
    NULL,       /* traverse */
    NULL,       /* clear */
    NULL
};


PyObject *
PyInit_libcrush(void)
{
    PyObject * mod = PyModule_Create(&moduledef);
    if (mod == NULL) {
        return NULL;
    }

    /* Initialize LibCrush */
    LibCrushType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&LibCrushType) < 0) {
        Py_DECREF(mod);
        return NULL;
    }

    Py_INCREF(&LibCrushType);
    if (PyModule_AddObject(mod, "LibCrush", (PyObject *)&LibCrushType) < 0) {
        Py_DECREF(mod);
        Py_DECREF(&LibCrushType);
        return NULL;
    }

    return mod;
}

#else

void
initlibcrush(void)
{
    PyObject * mod;

    mod = Py_InitModule3("crush.libcrush", NULL, MODULE_DOC);
    if (mod == NULL) {
        return;
    }

    /* Initialize LibCrush */
    LibCrushType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&LibCrushType) < 0) {
        return;
    }

    Py_INCREF(&LibCrushType);
    PyModule_AddObject(mod, "LibCrush", (PyObject *)&LibCrushType);

}

#endif /* Py3k */
