#include <iostream>
#include <sstream>
#include <fstream>

#include "include/assert.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"

using namespace ceph;

#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"

#include "ceph_read_write.h"

static char *crush_to_json(CrushWrapper& crush)
{
  boost::scoped_ptr<ceph::Formatter> f(ceph::Formatter::create("json-pretty", "json-pretty", "json-pretty"));
  f->open_object_section("crush_map");
  crush.dump(f.get());
  f->close_section();
  ostringstream sout;
  f->flush(sout);
  sout << "\n";
  return strdup(sout.str().c_str());
}

static int ceph_copy_choose_args(LibCrush *self, CrushWrapper &crush)
{
  PyObject *key;
  PyObject *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(self->choose_args, &pos, &key, &value)) {
    int k = MyInt_AsInt(key);
    if (PyErr_Occurred()) {
      crush.choose_args.clear();
      return -EINVAL;
    }
    struct crush_choose_arg_map choose_arg_map;
    choose_arg_map.args = (struct crush_choose_arg *)PyCapsule_GetPointer(value, NULL);
    choose_arg_map.size = crush.crush->max_buckets;
    crush.choose_args[k] = choose_arg_map;
  }
  return 0;
}

static int _ceph_write(LibCrush *self, const char *path, const char *format, PyObject *info, CrushWrapper &crush)
{
  PyObject *key;
  PyObject *value;
  Py_ssize_t pos;

  // type names
  pos = 0;
  while (PyDict_Next(self->types, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return -EINVAL;
    int v = MyInt_AsInt(value);
    if (PyErr_Occurred())
      return -EINVAL;
    crush.set_type_name(v, k);
  }

  // item names
  pos = 0;
  while (PyDict_Next(self->items, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return -EINVAL;
    int v = MyInt_AsInt(value);
    if (PyErr_Occurred())
      return -EINVAL;
    crush.set_item_name(v, k);
  }

  // rule names
  pos = 0;
  while (PyDict_Next(self->rules, &pos, &key, &value)) {
    const char *k = MyText_AsString(key);
    if (k == 0)
      return -EINVAL;
    int v = MyInt_AsInt(value);
    if (PyErr_Occurred())
      return -EINVAL;
    crush.set_rule_name(v, k);
  }

  int r = ceph_copy_choose_args(self, crush);
  if (r < 0)
    return r;

  if (info != Py_None) {
    PyObject *rules = PyDict_GetItemString(info, "rules");
    if (rules != NULL) {
      PyObject *self_rule_name;
      PyObject *python_rule_id;
      pos = 0;
      while (PyDict_Next(self->rules, &pos, &self_rule_name, &python_rule_id)) {
        int rule_id = MyInt_AsInt(python_rule_id);
        assert(rule_id >= 0);
        assert(rule_id < self->map->max_rules);
        Py_ssize_t i;
        for (i = 0; i < PyList_Size(rules); i++) {
          PyObject *python_rule = PyList_GetItem(rules, i);
          PyObject *python_rule_name = PyDict_GetItemString(python_rule, "rule_name");
          if (python_rule_name == NULL)
            continue;
          if (!PyObject_RichCompareBool(self_rule_name, python_rule_name, Py_EQ))
            continue;
          struct crush_rule *rule = self->map->rules[rule_id];
          assert(rule);
          PyObject *python_type = PyDict_GetItemString(python_rule, "type");
          assert(python_type);
          rule->mask.type = MyInt_AsInt(python_type);
          PyObject *python_min_size = PyDict_GetItemString(python_rule, "min_size");
          assert(python_min_size);
          rule->mask.min_size = MyInt_AsInt(python_min_size);
          PyObject *python_max_size = PyDict_GetItemString(python_rule, "max_size");
          assert(python_max_size);
          rule->mask.max_size = MyInt_AsInt(python_max_size);
          PyObject *python_ruleset = PyDict_GetItemString(python_rule, "ruleset");
          assert(python_ruleset);
          rule->mask.ruleset = MyInt_AsInt(python_ruleset);
        }
      }
    }
    PyObject *tunables = PyDict_GetItemString(info, "tunables");
    if (tunables != NULL) {
      PyObject *algs = PyDict_GetItemString(tunables, "allowed_bucket_algs");
      self->map->allowed_bucket_algs = MyInt_AsInt(algs);
    }
  }

  if (!strcmp(format, "txt")) {
    bool verbose = true;
    CrushCompiler cc(crush, std::cerr, verbose);
    ofstream o;
    o.open(path, ios::out | ios::binary | ios::trunc);
    if (!o.is_open()) {
      std::cerr << "error writing '" << path << "'" << std::endl;
      return -EINVAL;
    }
    cc.decompile(o);
    o.close();

  } else if (!strcmp(format, "crush")) {
    bufferlist bl;
    crush.encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
    int r = bl.write_file(path);
    if (r < 0) {
      std::cerr << "error writing '" << path << "'" << std::endl;
      return r;
    }

  } else if (!strcmp(format, "json")) {
    boost::scoped_ptr<Formatter> f(Formatter::create("json-pretty", "json-pretty", "json-pretty"));
    f->open_object_section("crush_map");
    crush.dump(f.get());
    f->close_section();
    ofstream o;
    o.open(path, ios::out | ios::binary | ios::trunc);
    if (!o.is_open()) {
      std::cerr << "error writing '" << path << "'" << std::endl;
      return -EINVAL;
    }
    f->flush(o);
    o.close();
  } else {
    return -EDOM;
  }

  return 0;
}

int ceph_write(LibCrush *self, const char *path, const char *format, PyObject *info)
{
  CrushWrapper crush;
  crush.crush = self->map;
  int r = _ceph_write(self, path, format, info, crush);
  crush.crush = NULL;
  crush.choose_args.clear();
  return r;
}

  return 0;
}

int ceph_read_txt_to_json(const char *in, char **out)
{
  ifstream fin(in);
  if (!fin.is_open())
    return -ENOENT;
  CrushWrapper crush;
  CrushCompiler cc(crush, cerr);
  int r = cc.compile(fin, in);
  if (r < 0)
    return r;
  *out = crush_to_json(crush);
  return 0;
}

int ceph_read_binary_to_json(const char *in, char **out)
{
  ceph::bufferlist bl;
  std::string error;
  int r = bl.read_file(in, &error);
  if (r < 0)
    return r;
  ceph::bufferlist::iterator p = bl.begin();
  CrushWrapper crush;
  try {
    crush.decode(p);
  } catch(...) {
    return -EINVAL;
  }
  *out = crush_to_json(crush);
  return 0;
}
