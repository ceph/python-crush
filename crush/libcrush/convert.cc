#include <iostream>
#include <sstream>
#include <fstream>

#include "include/assert.h"
#include "common/Formatter.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"

#include "convert.h"

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

int convert_txt_to_json(const char *in, char **out)
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

int convert_binary_to_json(const char *in, char **out)
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
}
