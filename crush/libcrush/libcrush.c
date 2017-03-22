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

#include <bytesobject.h>

#include "hash.h"
#include "builder.h"
#include "mapper.h"

#define RET_OK      0
#define RET_ERROR   -1

static int
LibCrush_init(LibCrush *self, PyObject *args, PyObject *kwds)
{
  self->verbose = 0;
  self->backward_compatibility = 0;

  static char *kwlist[] = {"verbose", "backward_compatibility", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ii", kwlist,
                                   &self->verbose,
                                   &self->backward_compatibility))
    return RET_ERROR;

  self->map = NULL;
  self->tunables = crush_create();

  if (self->tunables == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "crush_create() for tunables returned NULL");
    return 0;
  }
  self->types = PyDict_New();
  self->items = PyDict_New();
  self->ritems = PyDict_New();
  self->rules = PyDict_New();

  return RET_OK;
}

static void
LibCrush_dealloc(LibCrush *self)
{
  if (self->map != NULL)
    crush_destroy(self->map);
  if (self->tunables != NULL)
    crush_destroy(self->tunables);
  Py_DECREF(self->types);
  Py_DECREF(self->items);
  Py_DECREF(self->ritems);
  Py_DECREF(self->rules);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static int parse_type(LibCrush *self, PyObject *bucket, int *typeout, PyObject *trace)
{
  PyObject *type_name = PyDict_GetItemString(bucket, "type");
  if (type_name == NULL) {
    *typeout = -1;
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("type %S", type_name));
    if (MyText_AsString(type_name) == NULL)
      return 0;
    if (!PyDict_Contains(self->types, type_name)) {
      PyObject *value = MyInt_FromInt(PyDict_Size(self->types));
      PyDict_SetItem(self->types, type_name, value);
      Py_DECREF(value);
    }
    PyObject *type = PyDict_GetItem(self->types, type_name);
    *typeout = MyInt_AsInt(type);
    if (PyErr_Occurred())
      return 0;
  }
  return 1;
}

static int parse_bucket_type(LibCrush *self, PyObject *bucket, int *typeout, PyObject *trace)
{
  if (!PyDict_GetItemString(bucket, "type")) {
    PyErr_SetString(PyExc_RuntimeError, "missing type");
    return 0;
  }
  return parse_type(self, bucket, typeout, trace);
}

static int parse_bucket_id(LibCrush *self, PyObject *bucket, int *idout, PyObject *trace)
{
  PyObject *id = PyDict_GetItemString(bucket, "id");
  if (id == NULL) {
    *idout = crush_get_next_bucket_id(self->map);
    PyList_Append(trace, PyUnicode_FromFormat("id %d (default)", *idout));
    PyObject *python_id = MyInt_FromInt(*idout);
    int r = PyDict_SetItemString(bucket, "id", python_id);
    Py_DECREF(python_id);
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("id %S", id));
    *idout = MyInt_AsInt(id);
    if (PyErr_Occurred())
      return 0;
    if (*idout >= 0) {
      PyErr_Format(PyExc_RuntimeError, "id must be a negative integer, not %d", *idout);
      return 0;
    }
  }
  return 1;
}

static int parse_device_id(LibCrush *self, PyObject *bucket, int *idout, PyObject *trace)
{
  PyObject *id = PyDict_GetItemString(bucket, "id");
  if (id == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "missing id");
    return 0;
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("id %S", id));
    *idout = MyInt_AsInt(id);
    if (PyErr_Occurred())
      return 0;
    if (*idout < 0) {
      PyErr_Format(PyExc_RuntimeError, "id must be a positive integer, not %d", *idout);
      return 0;
    }
    if (*idout > self->highest_device_id)
      self->highest_device_id = *idout;
  }
  return 1;
}

static int parse_bucket_algorithm(LibCrush *self, PyObject *bucket, int *algorithmout, PyObject *trace)
{
  PyObject *algorithm = PyDict_GetItemString(bucket, "algorithm");
  if (algorithm == NULL) {
    *algorithmout = CRUSH_BUCKET_STRAW2;
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("algorithm %S", algorithm));
    const char *a = MyText_AsString(algorithm);
    if (a == NULL)
      return 0;
    if (!strcmp(a, "straw") && !self->backward_compatibility) {
      PyErr_Format(PyExc_RuntimeError, "algorithm straw requires backward_compatibility to be set");
      return 0;
    }

    if (!strcmp(a, "uniform"))
      *algorithmout = CRUSH_BUCKET_UNIFORM;
    else if (!strcmp(a, "list"))
      *algorithmout = CRUSH_BUCKET_LIST;
    else if (!strcmp(a, "straw"))
      *algorithmout = CRUSH_BUCKET_STRAW;
    else if (!strcmp(a, "straw2") )
      *algorithmout = CRUSH_BUCKET_STRAW2;
    else {
      PyErr_Format(PyExc_RuntimeError, "algorithm must be one of uniform, list, straw2 not %s", a);
      return 0;
    }
  }
  return 1;
}

static int parse_weight(LibCrush *self, PyObject *item, int *weightout, PyObject *trace)
{
  PyObject *weight = PyDict_GetItemString(item, "weight");
  if (weight == NULL) {
    *weightout = 0x10000;
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("weight %S", weight));
    double w = PyFloat_AsDouble(weight);
    if (PyErr_Occurred())
      return 0;
    *weightout = (int)(w * (double)0x10000);
  }
  return 1;
}

static int parse_name(LibCrush *self, PyObject *item, PyObject **nameout, PyObject *trace)
{
  *nameout = PyDict_GetItemString(item, "name");
  if (*nameout == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "missing name");
    return 0;
  }
  return MyText_AsString(*nameout) != 0;
}

static int parse_children(LibCrush *self, PyObject *item, PyObject **childrenout, PyObject *trace)
{
  *childrenout = PyDict_GetItemString(item, "children");
  if (*childrenout == NULL)
    return 1;
  if (!PyList_Check(*childrenout)) {
    PyErr_SetString(PyExc_RuntimeError, "children must be a list");
    return 0;
  }
  return 1;
}

static int set_item_name(LibCrush *self, PyObject *name, int id)
{
  PyObject *python_id = MyInt_FromInt(id);
  int r = PyDict_SetItem(self->items, name, python_id);
  Py_DECREF(python_id);
  if (r != 0)
    return 0;
  r = PyDict_SetItem(self->ritems, python_id, name);
  if (r != 0)
    return 0;
  return 1;
}

static int parse_bucket_or_device(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace);

static int parse_bucket(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("bucket content %S", bucket));
  int id;
  if (!parse_bucket_id(self, bucket, &id, trace))
    return 0;
  int type;
  if (!parse_bucket_type(self, bucket, &type, trace))
    return 0;
  int algorithm;
  if (!parse_bucket_algorithm(self, bucket, &algorithm, trace))
    return 0;
  int weight;
  if (!parse_weight(self, bucket, &weight, trace))
    return 0;
  PyObject *name;
  if (!parse_name(self, bucket, &name, trace))
    return 0;
  PyObject *children;
  if (!parse_children(self, bucket, &children, trace))
    return 0;

  struct crush_bucket *b;

  b = crush_make_bucket(self->map, algorithm, CRUSH_HASH_DEFAULT, type, 0, NULL, NULL);
  if (b == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "crush_make_bucket() returned NULL");
    return 0;
  }
  int r = crush_add_bucket(self->map, id, b, idout);
  if (r < 0) {
    PyErr_Format(PyExc_RuntimeError, "crush_add_bucket(id=%d) returned %d %s", id, r, strerror(-r));
    return 0;
  }
  if (id != *idout) {
    PyErr_Format(PyExc_RuntimeError, "crush_add_bucket(id=%d) unexpectedly allocated %d",
                 id, *idout);
    return 0;
  }

  if (!set_item_name(self, name, *idout))
    return 0;

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(bucket, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return 0;
    if (!(!strcmp("id", k) ||
          !strcmp("name", k) ||
          !strcmp("children", k) ||
          !strcmp("weight", k) ||
          !strcmp("type", k) ||
          !strcmp("algorithm", k)
          )) {
      PyErr_Format(PyExc_RuntimeError, "%s is not among id, name, children, weight, type, algorithm", k);
      return 0;
    }
  }

  if (children != NULL) {
    for (pos = 0; pos < PyList_Size(children); pos++) {
      PyObject *item = PyList_GetItem(children, pos);
      PyList_Append(trace, PyUnicode_FromFormat("bucket or device %S", item));
      if (!PyDict_Check(item)) {
        PyErr_Format(PyExc_RuntimeError, "must be a dict");
        return 0;
      }
      int child;
      int child_weight;
      int r = parse_bucket_or_device(self, item, &child, &child_weight, trace);
      if (r == 0)
        return 0;
      r = crush_bucket_add_item(self->map, b, child, child_weight);
      if (r < 0) {
        PyErr_Format(PyExc_RuntimeError, "crush_bucket_add_item returned %d %s", r, strerror(-r));
        return 0;
      }
    }
  }

  if (PyDict_GetItemString(bucket, "weight") == 0) {
    *weightout = b->weight;
  } else {
    self->has_bucket_weights = 1;
    *weightout = weight;
  }

  return 1;
}

static int has_reference_id(LibCrush *self, PyObject *bucket, PyObject *trace)
{
  return PyDict_GetItemString(bucket, "reference_id") != NULL;
}

static int parse_reference_id(LibCrush *self, PyObject *bucket, int *idout, PyObject *trace)
{
  PyObject *id = PyDict_GetItemString(bucket, "reference_id");
  if (id == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "missing reference_id");
    return 0;
  } else {
    PyList_Append(trace, PyUnicode_FromFormat("reference_id %S", id));
    *idout = MyInt_AsInt(id);
    if (PyErr_Occurred())
      return 0;
  }
  return 1;
}

static int parse_reference(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("reference content %S", bucket));

  if (!parse_reference_id(self, bucket, idout, trace))
    return 0;
  int weight;
  if (!parse_weight(self, bucket, weightout, trace))
    return 0;

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(bucket, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return 0;
    if (!(!strcmp("reference_id", k) ||
          !strcmp("weight", k))) {
      PyErr_Format(PyExc_RuntimeError, "%s is not among reference_id, weight", k);
      return 0;
    }
  }
  return 1;
}

static int parse_device(LibCrush *self, PyObject *device, int *idout, int *weightout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("device content %S", device));
  if (!parse_device_id(self, device, idout, trace))
    return 0;
  if (!parse_weight(self, device, weightout, trace))
    return 0;
  PyObject *name;
  if (!parse_name(self, device, &name, trace))
    return 0;

  if (!set_item_name(self, name, *idout))
    return 0;

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(device, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return 0;
    if (strcmp("id", k) && strcmp("weight", k) && strcmp("name", k)) {
      PyErr_Format(PyExc_RuntimeError, "'%s' is not among id, name, weight", k);
      return 0;
    }
  }
  return 1;
}

static int parse_bucket_or_device(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
  if (has_reference_id(self, bucket, trace))
    return parse_reference(self, bucket, idout, weightout, trace);
  int type;
  if (!parse_type(self, bucket, &type, trace))
    return 0;
  if (type == -1)
    return parse_device(self, bucket, idout, weightout, trace);
  else
    return parse_bucket(self, bucket, idout, weightout, trace);
}

static int print_trace(PyObject *trace)
{
  PyObject *f = PySys_GetObject("stdout");
  Py_ssize_t i;
  for (i = 0; i < PyList_Size(trace); i++) {
    const char *msg = MyText_AsString(PyList_GetItem(trace, i));
    if (PyFile_WriteString(msg, f) != 0)
      return 0;
    if (PyFile_WriteString("\n", f) != 0)
      return 0;
  }
  return 1;
}

static int reweight(LibCrush *self, int root, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("reweight bucket %d", root));
  if (root >= 0)
    return 1;
  int index = -1-root;
  if (index >= self->map->max_buckets) {
    PyErr_Format(PyExc_RuntimeError, "bucket id %d not in [0,%d[", root, self->map->max_buckets);
    return 0;
  }
  struct crush_bucket *b = self->map->buckets[index];
  if (b == NULL) {
    PyErr_Format(PyExc_RuntimeError, "no bucket with id %d", root);
    return 0;
  }
  int r = crush_reweight_bucket(self->map, b);
  if (r != 0) {
    PyErr_Format(PyExc_RuntimeError, "failed to reweight bucket %d %s", root, strerror(-r));
    return 0;
  } else {
    return 1;
  }
}

#define OPERANDS_SET "set_choose_tries set_choose_local_tries set_choose_local_fallback_tries set_chooseleaf_tries set_chooseleaf_vary_r set_chooseleaf_stable"
#define OPERANDS_CHOOSE "choose chooseleaf"
#define OPERANDS_OTHER "take emit"
#define OPERANDS_ALL OPERANDS_OTHER " " OPERANDS_SET " " OPERANDS_CHOOSE

static int parse_step_choose(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  Py_ssize_t len = PyList_Size(step);
  if (len != 5) {
    PyErr_Format(PyExc_RuntimeError, "must have exactly five elements, not %d", (int)len);
    return 0;
  }

  PyList_Append(trace, PyUnicode_FromFormat("step choose* %S", step));
  PyObject *python_op = PyList_GetItem(step, 0);
  const char *k = MyText_AsString(python_op);
  if (k == NULL)
    return 0;

  PyObject *python_op2 = PyList_GetItem(step, 1);
  const char *k2 = MyText_AsString(python_op2);
  if (k2 == NULL)
    return 0;

  int op;
  if (!strcmp("choose", k)) {
    if (!strcmp("firstn", k2))
      op = CRUSH_RULE_CHOOSE_FIRSTN;
    else if (!strcmp("indep", k2))
      op = CRUSH_RULE_CHOOSE_INDEP;
    else {
      PyErr_Format(PyExc_RuntimeError, "choose operand qualifier unknown %s, must be one of firstn indep", k2);
      return 0;
    }
  } else if (!strcmp("chooseleaf", k)) {
    if (!strcmp("firstn", k2))
      op = CRUSH_RULE_CHOOSELEAF_FIRSTN;
    else if (!strcmp("indep", k2))
      op = CRUSH_RULE_CHOOSELEAF_INDEP;
    else {
      PyErr_Format(PyExc_RuntimeError, "chooseleaf operand qualifier unknown %s, must be one of first indep", k2);
      return 0;
    }
  } else {
    PyErr_Format(PyExc_RuntimeError, "choose operand unknown %s, must be one of %s", k, OPERANDS_CHOOSE);
    return 0;
  }

  int replication_count = MyInt_AsInt(PyList_GetItem(step, 2));
  if (PyErr_Occurred())
    return 0;
  if (replication_count < 0) {
    PyErr_Format(PyExc_RuntimeError, "replication_count %d must be positive", replication_count);
    return 0;
  }

  const char *type_keyword = MyText_AsString(PyList_GetItem(step, 3));
  if (type_keyword == NULL || strcmp(type_keyword, "type")) {
    PyErr_Format(PyExc_RuntimeError, "third argument must be 'type'");
    return 0;
  }

  PyObject *python_type_reference = PyList_GetItem(step, 4);
  int type;
  if (MyText_Check(python_type_reference)) {
    if (!PyDict_Contains(self->types, python_type_reference)) {
      PyErr_Format(PyExc_RuntimeError, "type is unknown");
      return 0;
    }
    PyObject *python_type = PyDict_GetItem(self->types, python_type_reference);
    type = MyInt_AsInt(python_type);
    if (PyErr_Occurred())
      return 0;
  } else {
    type = MyInt_AsInt(python_type_reference);
    if (PyErr_Occurred())
      return 0;
  }

  crush_rule_set_step(crule, step_index, op, replication_count, type);

  return 1;
}

#define STEP_BACKWARD(keyword, upper_keyword)        \
    else if (!strcmp(k, #keyword)) { \
      if (self->backward_compatibility) { \
        op = upper_keyword; \
      } else { \
        PyErr_SetString(PyExc_RuntimeError, "not allowed unless backward_compatibility is set to 1"); \
        return 0; \
      } \
    }

static int parse_step_set(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step set_* %S", step));
  Py_ssize_t len = PyList_Size(step);
  if (len != 2) {
    PyErr_Format(PyExc_RuntimeError, "must have exactly two elements, not %d", (int)len);
    return 0;
  }
  PyObject *python_op = PyList_GetItem(step, 0);
  const char *k = MyText_AsString(python_op);
  if (k == NULL)
    return 0;
  int op;
  if (!strcmp("set_choose_tries", k))
    op = CRUSH_RULE_SET_CHOOSE_TRIES;
  else if (!strcmp("set_chooseleaf_tries", k))
    op = CRUSH_RULE_SET_CHOOSELEAF_TRIES;
  STEP_BACKWARD(set_choose_local_tries, CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES)
  STEP_BACKWARD(set_choose_local_fallback_tries, CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES)
  STEP_BACKWARD(set_chooseleaf_vary_r, CRUSH_RULE_SET_CHOOSELEAF_VARY_R)
  STEP_BACKWARD(set_chooseleaf_stable, CRUSH_RULE_SET_CHOOSELEAF_STABLE)
  else {
    PyErr_Format(PyExc_RuntimeError, "set operand unknown %s, must be one of %s", k, OPERANDS_SET);
    return 0;
  }

  int value = MyInt_AsInt(PyList_GetItem(step, 1));
  if (PyErr_Occurred())
    return 0;

  crush_rule_set_step(crule, step_index, op, value, 0);

  return 1;
}

static int parse_step_emit(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step emit %S", step));
  Py_ssize_t len = PyList_Size(step);
  if (len != 1) {
    PyErr_Format(PyExc_RuntimeError, "must have exactly one element, not %d", (int)len);
    return 0;
  }
  crush_rule_set_step(crule, step_index, CRUSH_RULE_EMIT, 0, 0);
  return 1;
}

static int parse_step_take(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step take %S", step));
  Py_ssize_t len = PyList_Size(step);
  if (len != 2) {
    PyErr_Format(PyExc_RuntimeError, "must have exactly two elements, not %d", (int)len);
    return 0;
  }
  PyObject *arg = PyList_GetItem(step, 1);
  if (MyText_AsString(arg) == NULL)
    return 0;
  PyObject *python_id = PyDict_GetItem(self->items, arg);
  if (python_id == NULL) {
    PyErr_Format(PyExc_RuntimeError, "not a known bucket or device");
    return 0;
  }
  int id = MyInt_AsInt(python_id);
  if (PyErr_Occurred())
    return 0;
  crush_rule_set_step(crule, step_index, CRUSH_RULE_TAKE, id, 0);
  return 1;
}

static int parse_step(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  Py_ssize_t len = PyList_Size(step);
  if (len < 1) {
    PyErr_SetString(PyExc_RuntimeError, "missing operand");
    return 0;
  }
  PyObject *op = PyList_GetItem(step, 0);
  const char *k = MyText_AsString(op);
  if (k == NULL)
    return 0;
  int r;
  if (!strcmp("take", k))
    r = parse_step_take(self, step, step_index, crule, trace);
  else if (!strcmp("emit", k))
    r = parse_step_emit(self, step, step_index, crule, trace);
  else if (!strncmp("set_", k, 4))
    r = parse_step_set(self, step, step_index, crule, trace);
  else if (!strncmp("choose", k, 6))
    r = parse_step_choose(self, step, step_index, crule, trace);
  else {
    PyErr_Format(PyExc_RuntimeError, "operand unknown %s, must be one of %s", k, OPERANDS_ALL);
    return 0;
  }

  return r;
}

static int parse_steps(LibCrush *self, PyObject *rule, struct crush_rule *crule, PyObject *trace)
{
  Py_ssize_t i;
  for (i = 0; i < PyList_Size(rule); i++) {
     PyObject *step = PyList_GetItem(rule, i);
     PyList_Append(trace, PyUnicode_FromFormat("step %d %S", i, step));
     int r = parse_step(self, step, i, crule, trace);
     if (!r)
       return 0;
  }
  return 1;
}

static int parse_rule(LibCrush *self, PyObject *name, PyObject *rule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("rule content %S", rule));
  int steps_size = PyList_Size(rule);

  int minsize = 0;
  int maxsize = 0;
  struct crush_rule *crule = crush_make_rule(steps_size, 0, 0, minsize, maxsize);
  if (crule == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "crush_make_rule() returned NULL");
    return 0;
  }

  int ruleno = crush_add_rule(self->map, crule, -1);
  if (ruleno < 0) {
    PyErr_Format(PyExc_RuntimeError, "crush_add_rule(%s) failed %d %s", MyText_AsString(name), ruleno, strerror(-ruleno));
    return 0;
  }
  PyObject *python_ruleno = MyInt_FromInt(ruleno);
  int r = PyDict_SetItem(self->rules, name, python_ruleno);
  Py_DECREF(python_ruleno);
  if (r != 0)
    return 0;

  r = parse_steps(self, rule, crule, trace);
  if (!r)
    return r;

  return 1;
}

static int parse_rules(LibCrush *self, PyObject *map, PyObject *trace)
{
  PyObject *rules = PyDict_GetItemString(map, "rules");
  if (rules == NULL)
    return 1;

  PyList_Append(trace, PyUnicode_FromFormat("rules %S", rules));

  if (!PyDict_Check(rules)) {
    PyErr_Format(PyExc_RuntimeError, "must be a dict");
    return 0;
  }

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(rules, &pos, &key, &value)) {
    PyList_Append(trace, PyUnicode_FromFormat("rule name %S", key));
    if (MyText_AsString(key) == NULL)
      return 0;
    int r = parse_rule(self, key, value, trace);
    if (!r)
      return 0;
  }
  return 1;
}

static int parse_trees(LibCrush *self, PyObject *map, PyObject *trace)
{
  PyObject *trees = PyDict_GetItemString(map, "trees");
  if (trees == NULL)
    return 1;

  PyList_Append(trace, PyUnicode_FromFormat("trees %S", trees));

  if (!PyList_Check(trees)) {
    PyErr_Format(PyExc_RuntimeError, "must be a list");
    return 0;
  }

  PyDict_Clear(self->types);
  PyDict_Clear(self->items);
  PyDict_Clear(self->ritems);
  self->highest_device_id = -1;

  Py_ssize_t pos;
  for (pos = 0; pos < PyList_Size(trees); pos++) {
    PyObject *root = PyList_GetItem(trees, pos);
    PyList_Append(trace, PyUnicode_FromFormat("root %S", root));

    int id;
    int weight;
    self->has_bucket_weights = 0;
    int r = parse_bucket(self, root, &id, &weight, trace);
    if (!r)
      return 0;

    if (!self->has_bucket_weights) {
      PyList_Append(trace, PyUnicode_FromFormat("reweight"));
      r = reweight(self, id, trace);
      if (!r)
        return 0;
    }
  }
  return 1;
}

#define PARSE_BACKWARD(keyword) \
    else if (!strcmp(key, #keyword)) { \
      if (self->backward_compatibility) { \
        self->tunables->keyword = value; \
      } else { \
        PyErr_SetString(PyExc_RuntimeError, "not allowed unless backward_compatibility is set to 1"); \
        return 0; \
      } \
    }

static int parse_tunables(LibCrush *self, PyObject *map, PyObject *trace)
{
  PyObject *tunables = PyDict_GetItemString(map, "tunables");
  if (tunables == NULL)
    return 1;

  PyList_Append(trace, PyUnicode_FromFormat("tunables %S", tunables));

  if (!PyDict_Check(tunables)) {
    PyErr_Format(PyExc_RuntimeError, "must be a dict");
    return 0;
  }

  self->tunables->choose_local_tries = 0;
  self->tunables->choose_local_fallback_tries = 0;
  self->tunables->chooseleaf_descend_once = 1;
  self->tunables->chooseleaf_vary_r = 1;
  self->tunables->chooseleaf_stable = 1;
  self->tunables->straw_calc_version = 1;
  self->tunables->choose_total_tries = 50;

  PyObject *python_key;
  PyObject *python_value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(tunables, &pos, &python_key, &python_value)) {
    PyList_Append(trace, PyUnicode_FromFormat("tunable %S = %S", python_key, python_value));
    const char *key = MyText_AsString(python_key);
    if (key == NULL)
      return 0;
    int value = MyInt_AsInt(python_value);
    if (PyErr_Occurred())
      return 0;
    if (!strcmp(key, "choose_total_tries"))
      self->tunables->choose_total_tries = value;
    PARSE_BACKWARD(choose_local_tries)
    PARSE_BACKWARD(choose_local_fallback_tries)
    PARSE_BACKWARD(chooseleaf_vary_r)
    PARSE_BACKWARD(chooseleaf_stable)
    PARSE_BACKWARD(chooseleaf_descend_once)
    PARSE_BACKWARD(straw_calc_version)
    else {
      PyErr_Format(PyExc_RuntimeError, "unknown tunable %s", key);
      return 0;                                                         \
    }
  }

  return 1;
}

static int parse(LibCrush *self, PyObject *map, PyObject *trace)
{
  int r = parse_trees(self, map, trace);
  if (!r)
    return 0;

  r = parse_rules(self, map, trace);
  if (!r)
    return 0;

  r = parse_tunables(self, map, trace);
  if (!r)
    return 0;

  crush_finalize(self->map);

  return 1;
}

static PyObject *
LibCrush_parse(LibCrush *self, PyObject *args)
{
  PyObject *map;

  if (!PyArg_ParseTuple(args, "O!", &PyDict_Type, &map))
    return 0;

  if (self->map != NULL)
    crush_destroy(self->map);
  self->map = crush_create();

  if (self->map == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "crush_create() returned NULL");
    return 0;
  }

  PyObject *trace = PyList_New(0);
  int r = parse(self, map, trace);
  if (!r || self->verbose)
    print_trace(trace);
  Py_DECREF(trace);

  if (!r)
    return 0;

  Py_RETURN_TRUE;
}

static int print_debug(PyObject *message)
{
  if (message == NULL)
    return 0;
  PyObject *out = PySys_GetObject("stdout");
  int r = PyFile_WriteString(MyText_AsString(message), out);
  return r == 0;
}

static PyObject *
LibCrush_map(LibCrush *self, PyObject *args, PyObject *kwds)
{
  PyObject *rule;
  int value;
  int replication_count = -1;
  PyObject *python_weights = NULL;
  static char *kwlist[] = {
    "rule", "value", "replication_count", "weights", NULL
  };
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!iI|O!", kwlist,
                                   &MyText_Type, &rule,
                                   &value,
                                   &replication_count,
                                   &PyDict_Type, &python_weights))
    return 0;

  if (self->map == NULL) {
    PyErr_Format(PyExc_RuntimeError, "call parse() before map()");
    return 0;
  }
  if (replication_count < 1) {
    PyErr_Format(PyExc_RuntimeError, "replication_count %d must be >= 1", replication_count);
    return 0;
  }
  PyObject *python_ruleno = PyDict_GetItem(self->rules, rule);
  if (python_ruleno == NULL) {
    PyErr_Format(PyExc_RuntimeError, "rule %s is not found", MyText_AsString(rule));
    return 0;
  }
  int ruleno = MyInt_AsInt(python_ruleno);
  if (PyErr_Occurred())
    return 0;

  if (self->verbose)
    print_debug(PyUnicode_FromFormat("map(rule=%S=%d, value=%d, replication_count=%d)\n",
                                     rule,
                                     ruleno,
                                     value,
                                     replication_count));

  self->map->choose_local_tries = self->tunables->choose_local_tries;
  self->map->choose_local_fallback_tries = self->tunables->choose_local_fallback_tries;
  self->map->chooseleaf_descend_once = self->tunables->chooseleaf_descend_once;
  self->map->chooseleaf_vary_r = self->tunables->chooseleaf_vary_r;
  self->map->chooseleaf_stable = self->tunables->chooseleaf_stable;
  self->map->straw_calc_version = self->tunables->straw_calc_version;
  self->map->choose_total_tries = self->tunables->choose_total_tries;

  self->map->allowed_bucket_algs =
    (1 << CRUSH_BUCKET_UNIFORM) |
    (1 << CRUSH_BUCKET_LIST) |
    (1 << CRUSH_BUCKET_STRAW2);

  if (self->backward_compatibility) {
    self->map->allowed_bucket_algs =
      self->map->allowed_bucket_algs |
      (1 << CRUSH_BUCKET_STRAW);
  }

  int weights_size = self->highest_device_id + 1;
  __u32 weights[weights_size];
  int i;
  for (i = 0; i < weights_size; i++)
    weights[i] = 0x10000;

  if (python_weights != NULL) {
    PyObject *device;
    PyObject *new_weight;
    Py_ssize_t pos = 0;
    while (PyDict_Next(python_weights, &pos, &device, &new_weight)) {
      PyObject *python_id = PyDict_GetItem(self->items, device);
      if (python_id == NULL) {
        PyErr_Format(PyExc_RuntimeError, "%s is not a known device", MyText_AsString(device));
        return 0;
      }
      int id = MyInt_AsInt(python_id);
      if (PyErr_Occurred())
        return 0;
      if (id >= weights_size) {
        PyErr_Format(PyExc_RuntimeError, "%s id %d is greater than weights_size %d", MyText_AsString(device), id, weights_size);
        return 0;
      }
      double weightf = PyFloat_AsDouble(new_weight);
      if (PyErr_Occurred())
        return 0;
      int weight = (int)(weightf * (double)0x10000);
      weights[id] = weight;
    }
  }

  int result[replication_count];
  memset(result, '\0', sizeof(int) * replication_count);
  int cwin_size = crush_work_size(self->map, replication_count);
  char cwin[cwin_size];
  crush_init_workspace(self->map, cwin);

  int result_len = crush_do_rule(self->map,
                                 ruleno,
                                 value,
                                 result, replication_count,
                                 weights, weights_size,
                                 cwin);
  if (result_len == 0) {
    PyErr_Format(PyExc_RuntimeError, "crush_do_rule() was unable to map %d to any device", value);
    return 0;
  }

  PyObject *python_results = PyList_New(result_len);
  for (i = 0; i < result_len; i++) {
    PyObject *python_result;
    if (result[i] == CRUSH_ITEM_NONE) {
      python_result = Py_None;
    } else {
      PyObject *python_id = MyInt_FromInt(result[i]);
      if (PyErr_Occurred())
        return 0;
      python_result = PyDict_GetItem(self->ritems, python_id);
      Py_DECREF(python_id);
      if (python_result == NULL) {
        PyErr_Format(PyExc_RuntimeError, "%d does not map to a device name", result[i]);
        return 0;
      }
    }
    Py_INCREF(python_result); // because SetItem steals a reference
    int r = PyList_SetItem(python_results, i, python_result);
    if (r == -1)
      return 0;
  }
  return python_results;
}

#include "convert.h"

static PyObject *
LibCrush_convert(LibCrush *self, PyObject *args)
{
  const char *in;
  if (!PyArg_ParseTuple(args, "s", &in))
    return 0;

  char *out = NULL;
  int r;
  r = convert_binary_to_json(in, &out);
  if (r < 0)
    r = convert_txt_to_json(in, &out);
  if (r < 0) {
    PyErr_Format(PyExc_RuntimeError, "%s is neither a text or binary Ceph crushmap", in);
    return 0;
  }
  PyObject *result = Py_BuildValue("s", out);
  free(out);
  return result;
}

static PyMemberDef
LibCrush_members[] = {
    { NULL }
};

static PyMethodDef
LibCrush_methods[] = {
    { "parse",      (PyCFunction) LibCrush_parse,    METH_VARARGS,
            PyDoc_STR("parse the crush map") },
    { "map",      (PyCFunction) LibCrush_map,        METH_VARARGS|METH_KEYWORDS,
            PyDoc_STR("map a value to items") },
    { "convert",  (PyCFunction) LibCrush_convert,    METH_VARARGS,
            PyDoc_STR("convert from Ceph txt crushmap ") },
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
