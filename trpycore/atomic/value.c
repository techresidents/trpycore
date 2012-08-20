#include <Python.h>
#include "structmember.h"

/**
 * Include CAS header files.
 */
#if __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ >= 1050
    #include <libkern/OSAtomic.h>
#elif defined(_MSC_VER)
#elif (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) > 40100
#else
#error No CAS operation available for this platform
#endif

typedef struct AtomicValue {
    PyObject_HEAD
    PyObject *value;
} AtomicValue;

static int AtomicValue_traverse(PyObject *self, visitproc visit, void *arg) {
    AtomicValue *av = (AtomicValue *) self;
    Py_VISIT(av->value);
    return 0;
}

static int AtomicValue_clear(PyObject *self) {
    AtomicValue *av = (AtomicValue *) self;
    Py_CLEAR(av->value);
    return 0;
}

static void AtomicValue_dealloc(PyObject *self) {
    AtomicValue_clear(self);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject* AtomicValue_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    AtomicValue *self = (AtomicValue *) type->tp_alloc(type, 0);
    if(self != NULL) {
        self->value = NULL;
    }
    return (PyObject *) self;
}

static int AtomicValue_init(AtomicValue *self, PyObject *args, PyObject *kwargs) {
    PyObject *value;
    if(!PyArg_ParseTuple(args, "O", &value)) {
        return -1;
    }
    self->value = value;
    Py_INCREF(self->value);
    return 0;
}

static PyObject* AtomicValue_get(AtomicValue* self) {
    Py_INCREF(self->value);
    return self->value;
}

static PyObject* AtomicValue_set(AtomicValue *self, PyObject *args) {
    PyObject *new_value;
    PyObject *old_value;
    if(!PyArg_ParseTuple(args, "O", &new_value)) {
        return NULL;
    }
    old_value = self->value;
    Py_INCREF(old_value);

    self->value = new_value;
    Py_INCREF(self->value);

    return old_value;
}

/**
 * Atomic compare and set method.
 * Note that these operations will be in all python interpreters,
 * even those without a GIL.
 *
 * Returns True if value is updated, False otherwise.
 */
static PyObject* AtomicValue_compare_and_set(AtomicValue *self, PyObject *args) {
    PyObject *expected_value;
    PyObject *new_value;
    if(!PyArg_ParseTuple(args, "OO", &expected_value, &new_value)) {
        return NULL;
    }

    Py_INCREF(self->value);
    Py_INCREF(expected_value);
    Py_INCREF(new_value);

#if __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ >= 1050
    if (OSAtomicCompareAndSwapPtr(expected_value, new_value, (void *) &self->value)) {
        Py_INCREF(Py_True);
        return Py_True;
    }
#elif defined(_MSC_VER)
    if (InterlockedCompareExchange(&self->value, new_value, old_value)) {
        Py_INCREF(Py_True);
        return Py_True;
    }
#elif (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) > 40100
    if (__sync_bool_compare_and_swap(&self->value, expected_value, new_value)) {
        Py_INCREF(Py_True);
        return Py_True;
    }
#else
#error No CAS operation available for this platform
#endif
    Py_INCREF(Py_False);
    return Py_False;
}

static PyMemberDef value_members[] = {
    {"value", T_OBJECT, offsetof(AtomicValue, value), READONLY, "value"},
    {NULL}
};

static PyMethodDef value_methods[] = {
    {"get", (PyCFunction) AtomicValue_get, METH_NOARGS, "Get value"},
    {"set", (PyCFunction) AtomicValue_set, METH_VARARGS, "Set value"},
    {"compare_and_set", (PyCFunction) AtomicValue_compare_and_set, METH_VARARGS, "Compare and set value"},
    {NULL}
};

#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size) \
    PyObject_HEAD_INIT(type) size,
#endif

static PyTypeObject AtomicValueType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "atomic.value.AtomicValue",
    sizeof(AtomicValue),
    0,
    AtomicValue_dealloc,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    "Atomic value object",
    AtomicValue_traverse,
    AtomicValue_clear,
    0,
    0,
    0,
    0,
    value_methods,
    value_members,
    0,
    0,
    0,
    0,
    0,
    0,
    (initproc) AtomicValue_init,
    0,
    AtomicValue_new,
    0,
};

#if PY_MAJOR_VERSION >= 3
#define MOD_ERROR_VAL NULL
#define MOD_SUCCESS_VAL(val) val
#define MOD_INIT(name) PyMODINIT_FUNC PyInit__##name(void)
#define MOD_DEF(ob, name, doc) \
    static struct PyModuleDef moduledef = { \
        PyModuleDef_HEAD_INIT, name, doc, -1}; \
ob = PyModule_Create(&moduledef);
#else
#define MOD_ERROR_VAL
#define MOD_SUCCESS_VAL(val)
#define MOD_INIT(name) void init##name(void)
#define MOD_DEF(ob, name, doc) \
    ob = Py_InitModule3(name, NULL, doc);
#endif

MOD_INIT(value) {
    PyObject *m;

    MOD_DEF(m, "value", "Atomic value module");
    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    AtomicValueType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&AtomicValueType) < 0) {
        return MOD_ERROR_VAL;
    }

    Py_INCREF(&AtomicValueType);
    PyModule_AddObject(m, "AtomicValue", (PyObject *) &AtomicValueType);

    return MOD_SUCCESS_VAL(m);
}
