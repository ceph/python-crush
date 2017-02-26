//
// Copyright (C) 2017 <contact@redhat.com>
//
// Author: Loic Dachary <loic@dachary.org>
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
//
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
