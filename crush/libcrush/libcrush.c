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

  static char *kwlist[] = {"verbose", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist,
                                   &self->verbose))
    return RET_ERROR;

  self->map = NULL;
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
  Py_DECREF(self->types);
  Py_DECREF(self->items);
  Py_DECREF(self->ritems);
  Py_DECREF(self->rules);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static const char *mm(char *fmt, ...)
{
  int size = 0;
  char *p = NULL;
  va_list ap;

  /* Determine required size */

  va_start(ap, fmt);
  size = vsnprintf(p, size, fmt, ap);
  va_end(ap);

  if (size < 0)
    return NULL;

  size++;             /* For '\0' */
  p = malloc(size);
  if (p == NULL)
    return NULL;

  va_start(ap, fmt);
  size = vsnprintf(p, size, fmt, ap);
  if (size < 0) {
    free(p);
    return NULL;
  }
  va_end(ap);

  return p;
}

static int parse_type(LibCrush *self, PyObject *bucket, int *typeout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("~type~"));
  PyObject *type_name = PyDict_GetItemString(bucket, "~type~");
  if (type_name == NULL) {
    *typeout = -1;
  } else {
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
  int r = parse_type(self, bucket, typeout, trace);
  if (!r)
    PyErr_SetString(PyExc_RuntimeError, "missing ~type~");
  return r;
}

static int parse_bucket_id(LibCrush *self, PyObject *bucket, int *idout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("~id~"));
  PyObject *id = PyDict_GetItemString(bucket, "~id~");
  if (id == NULL) {
    *idout = crush_get_next_bucket_id(self->map);
  } else {
    *idout = MyInt_AsInt(id);
    if (PyErr_Occurred())
      return 0;
  }
  return 1;
}

static int parse_device_id(LibCrush *self, PyObject *bucket, int *idout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("~id~"));
  PyObject *id = PyDict_GetItemString(bucket, "~id~");
  if (id == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "missing ~id~");
    return 0;
  } else {
    *idout = MyInt_AsInt(id);
    if (PyErr_Occurred())
      return 0;
    if (*idout > self->highest_device_id)
      self->highest_device_id = *idout;
  }
  return 1;
}

static int parse_bucket_algorithm(LibCrush *self, PyObject *bucket, int *algorithmout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("~algorithm~"));
  PyObject *algorithm = PyDict_GetItemString(bucket, "~algorithm~");
  if (algorithm == NULL) {
    *algorithmout = CRUSH_BUCKET_STRAW2;
  } else {
    const char *a = MyText_AsString(algorithm);
    if (!strcmp(a, "uniform"))
      *algorithmout = CRUSH_BUCKET_UNIFORM;
    else if (!strcmp(a, "list"))
      *algorithmout = CRUSH_BUCKET_LIST;
    else if (!strcmp(a, "straw2") )
      *algorithmout = CRUSH_BUCKET_STRAW2;
    else {
      PyErr_Format(PyExc_RuntimeError, "~algorithm~ must be one of uniform, list, straw not %s", a);
      return 0;
    }
  }
  return 1;
}

static int parse_weight(LibCrush *self, PyObject *item, int *weightout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("~weight~"));
  PyObject *weight = PyDict_GetItemString(item, "~weight~");
  if (weight == NULL) {
    *weightout = 0x10000;
  } else {
    double w = PyFloat_AsDouble(weight);
    if (PyErr_Occurred())
      return 0;
    *weightout = (int)(w * (double)0x10000);
  }
  return 1;
}

static int parse_bucket_or_device(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace);

static int parse_bucket(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
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

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(bucket, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return 0;
    if (strlen(k) > 1 && k[0] == '~') {
      if (!(!strcmp("~id~", k) ||
            !strcmp("~weight~", k) ||
            !strcmp("~weight~", k) ||
            !strcmp("~type~", k) ||
            !strcmp("~algorithm~", k)
            )) {
        PyErr_Format(PyExc_RuntimeError, "%s is not among ~id~, ~weight~, ~type~, ~algorithm~", k);
        return 0;
      }
      continue;
    }
    PyList_Append(trace, key);
    int child;
    int child_weight;
    int r = parse_bucket_or_device(self, value, &child, &child_weight, trace);
    if (r == 0)
      return 0;
    r = crush_bucket_add_item(self->map, b, child, child_weight);
    if (r < 0) {
      PyErr_Format(PyExc_RuntimeError, "crush_bucket_add_item(%s) returned %d %s", k, r, strerror(-r));
      return 0;
    }
    PyObject *python_child = MyInt_FromInt(child);
    r = PyDict_SetItem(self->items, key, python_child);
    Py_DECREF(python_child);
    if (r != 0)
      return 0;
    r = PyDict_SetItem(self->ritems, python_child, key);
    if (r != 0)
      return 0;
  }

  if (PyDict_GetItemString(bucket, "~weight~") == 0) {
    *weightout = b->weight;
  } else {
    self->has_bucket_weights = 1;
    *weightout = weight;
  }

  return 1;
}

static int parse_device(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
  if (!parse_device_id(self, bucket, idout, trace))
    return 0;
  if (!parse_weight(self, bucket, weightout, trace))
    return 0;

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(bucket, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return 0;
    if (strlen(k) == 0 || !(!strcmp("~id~", k) || !strcmp("~weight~", k))) {
      PyErr_Format(PyExc_RuntimeError, "'%s' is not among ~id~, ~weight~", k);
      return 0;
    }
  }
  return 1;
}

static int parse_bucket_or_device(LibCrush *self, PyObject *bucket, int *idout, int *weightout, PyObject *trace)
{
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
  for (Py_ssize_t i = 0; i < PyList_Size(trace); i++) {
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
    PyErr_Format(PyExc_RuntimeError, "bucket id %d out of range", root);
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

  PyList_Append(trace, PyUnicode_FromFormat("step choose*"));
  PyObject *python_op = PyList_GetItem(step, 0);
  const char *k = MyText_AsString(python_op);
  if (k == NULL)
    return 0;

  if (len < 2) {
    PyErr_SetString(PyExc_RuntimeError, "missing firstn/indep");
    return 0;
  }
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
      PyErr_Format(PyExc_RuntimeError, "choose operand qualifier unknown %s, must be one of first indep", k2);      
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

  if (len < 5) {
    PyErr_SetString(PyExc_RuntimeError, "missing arguments");
    return 0;
  }

  int replication_count = MyInt_AsInt(PyList_GetItem(step, 2));
  if (PyErr_Occurred())
    return 0;

  PyObject *python_type_name = PyList_GetItem(step, 4);
  const char *type_name = MyText_AsString(python_type_name);
  if (type_name == NULL)
    return 0;
  if (!PyDict_Contains(self->types, python_type_name)) {
    PyErr_Format(PyExc_RuntimeError, "type %s is unknown", type_name);
    return 0;
  }
  PyObject *python_type = PyDict_GetItem(self->types, python_type_name);
  int type = MyInt_AsInt(python_type);
  if (PyErr_Occurred())
    return 0;

  crush_rule_set_step(crule, step_index, op, replication_count, type);
  
  return 1;
}

static int parse_step_set(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step set_*"));
  PyObject *python_op = PyList_GetItem(step, 0);
  const char *k = MyText_AsString(python_op);
  if (k == NULL)
    return 0;
  int op;
  if (!strcmp("set_choose_tries", k))
    op = CRUSH_RULE_SET_CHOOSE_TRIES;
  else if (!strcmp("set_choose_local_tries", k))
    op = CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES;
  else if (!strcmp("set_choose_local_fallback_tries", k))
    op = CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES;
  else if (!strcmp("set_chooseleaf_tries", k))
    op = CRUSH_RULE_SET_CHOOSELEAF_TRIES;
  else if (!strcmp("set_chooseleaf_vary_r", k))
    op = CRUSH_RULE_SET_CHOOSELEAF_VARY_R;
  else if (!strcmp("set_chooseleaf_stable", k))
    op = CRUSH_RULE_SET_CHOOSELEAF_STABLE;
  else {
    PyErr_Format(PyExc_RuntimeError, "set operand unknown %s, must be one of %s", k, OPERANDS_SET);
    return 0;
  }

  Py_ssize_t len = PyList_Size(step);
  if (len < 2) {
    PyErr_SetString(PyExc_RuntimeError, "missing argument");
    return 0;
  }
  int value = MyInt_AsInt(PyList_GetItem(self->items, 1));

  crush_rule_set_step(crule, step_index, op, value, 0);
  
  return 1;
}

static int parse_step_emit(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step emit"));
  crush_rule_set_step(crule, step_index, CRUSH_RULE_EMIT, 0, 0);
  return 1;
}

static int parse_step_take(LibCrush *self, PyObject *step, int step_index, struct crush_rule *crule, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("step take"));
  Py_ssize_t len = PyList_Size(step);
  if (len < 2) {
    PyErr_SetString(PyExc_RuntimeError, "missing argument");
    return 0;
  }
  PyObject *arg = PyList_GetItem(step, 1);
  if (!PyUnicode_Check(arg)) {
    PyErr_SetString(PyExc_RuntimeError, "argument must be a string");
    return 0;
  }
  int id = MyInt_AsInt(PyDict_GetItem(self->items, arg));
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
  PyList_Append(trace, PyUnicode_FromFormat("steps"));
  PyObject *steps = PyDict_GetItemString(rule, "steps");
  for (Py_ssize_t i = 0; i < PyList_Size(steps); i++) {
     PyObject *step = PyList_GetItem(steps, i);
     PyList_Append(trace, PyUnicode_FromFormat("step %d", i));     
     int r = parse_step(self, step, i, crule, trace);
     if (!r)
       return 0;
  }
  return 1;
}
  
static int parse_steps_size(LibCrush *self, PyObject *rule, int *sizeout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat("steps size"));
  PyObject *steps = PyDict_GetItemString(rule, "steps");
  if (steps == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "missing steps");
    return 0;
  }

  *sizeout = PyList_Size(steps);

  return 1;
}

static int parse_rule_size(LibCrush *self, PyObject *rule, const char *name, int *sizeout, PyObject *trace)
{
  PyList_Append(trace, PyUnicode_FromFormat(name));
  PyObject *size = PyDict_GetItemString(rule, name);
  if (size == NULL) {
    PyErr_Format(PyExc_RuntimeError, "missing %s", name);
    return 0;
  } else {
    *sizeout = MyInt_AsInt(size);
    if (PyErr_Occurred())
      return 0;
  }
  return 1;
}

static int parse_rule(LibCrush *self, PyObject *name, PyObject *rule, PyObject *trace)
{
  PyList_Append(trace, name);
  int minsize;
  if (!parse_rule_size(self, rule, "min_size", &minsize, trace))
    return 0;
  int maxsize;
  if (!parse_rule_size(self, rule, "max_size", &maxsize, trace))
    return 0;
  int steps_size;
  if (!parse_steps_size(self, rule, &steps_size, trace))
    return 0;

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
  PyList_Append(trace, PyUnicode_FromFormat("rules"));
  PyObject *rules = PyDict_GetItemString(map, "rules");
  if (rules == NULL)
    return 1;

  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(rules, &pos, &key, &value)) {
    int r = parse_rule(self, key, value, trace);
    if (!r)
      return 0;
  }
  return 1;
}
                       
static int parse(LibCrush *self, PyObject *map, PyObject *trace)
{
  PyObject *buckets = PyDict_GetItemString(map, "buckets");
  if (buckets == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "the root of the crush map does not have a buckets key");
    return 0;
  }

  PyList_Append(trace, PyUnicode_FromFormat("buckets"));
  PyDict_Clear(self->types);
  PyDict_Clear(self->items);
  PyDict_Clear(self->ritems);  
  self->highest_device_id = -1;
               
  int id;
  int weight;
  int r = parse_bucket_or_device(self, buckets, &id, &weight, trace);
  if (!r)
    return 0;

  PyObject *python_id = MyInt_FromInt(id);
  r = PyDict_SetItemString(self->items, "buckets", python_id);
  Py_DECREF(python_id);
  if (r != 0)
    return 0;
  r = PyDict_SetItem(self->ritems, python_id, buckets);
  if (r != 0)
    return 0;

  if (!self->has_bucket_weights) {
    PyList_Append(trace, PyUnicode_FromFormat("reweight"));
    r = reweight(self, id, trace);
    if (!r)
      return 0;
  }

  r = parse_rules(self, map, trace);
  
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

  self->map->choose_local_tries = 0;
  self->map->choose_local_fallback_tries = 0;
  self->map->choose_total_tries = 50;
  self->map->chooseleaf_descend_once = 1;
  self->map->chooseleaf_vary_r = 1;
  self->map->chooseleaf_stable = 1;
  self->map->allowed_bucket_algs =
    (1 << CRUSH_BUCKET_UNIFORM) |
    (1 << CRUSH_BUCKET_LIST) |
    (1 << CRUSH_BUCKET_STRAW2);

  self->has_bucket_weights = 0;

  PyObject *trace = PyList_New(0);
  int r = parse(self, map, trace);
  if (!r || self->verbose)
    print_trace(trace);
  Py_DECREF(trace);

  if (!r)
    return 0;
  
  Py_RETURN_TRUE;
}

static int print_debug(LibCrush *self, char *message)
{
  if (self->verbose == 0)
    return 1;
  if (message == NULL)
    return 0;
  PyObject *out = PySys_GetObject("stdout");
  int r = PyFile_WriteString(message, out);
  free(message);
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

  PyObject *python_ruleno = PyDict_GetItem(self->rules, rule);
  if (python_ruleno == NULL) {
    PyErr_Format(PyExc_RuntimeError, "rule %s is not found", MyText_AsString(rule));
    return 0;
  }
  int ruleno = MyInt_AsInt(python_ruleno);
  if (PyErr_Occurred())
    return 0;

  print_debug(self, mm("map(rule=%s=%d, value=%d, replication_count=%d)", 
                       MyText_AsString(rule),
                       ruleno,
                       value,
                       replication_count));

  int weights_size = self->highest_device_id + 1;
  __u32 weights[weights_size];
  for(int i = 0; i < weights_size; i++)
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
  for (int i = 0; i < result_len; i++) {
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

static PyMemberDef
LibCrush_members[] = {
    { NULL }
};

static PyMethodDef
LibCrush_methods[] = {
    { "parse",      (PyCFunction) LibCrush_parse,        METH_VARARGS,
            PyDoc_STR("parse the crush map") },
    { "map",      (PyCFunction) LibCrush_map,        METH_VARARGS|METH_KEYWORDS,
            PyDoc_STR("map a value to items") },
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
