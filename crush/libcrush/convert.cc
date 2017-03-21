#include <iostream>
#include <fstream>

#include "common/Formatter.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"

#include "convert.h"

int convert_txt(const char *in, const char *out)
{
  ifstream fin(in);
  if (!fin.is_open()) {
    cerr << "input file " << in << " not found" << std::endl;
    return -1;
  }
  CrushWrapper crush;
  CrushCompiler cc(crush, cerr);
  cc.compile(fin, in);
  boost::scoped_ptr<ceph::Formatter> f(ceph::Formatter::create("json-pretty", "json-pretty", "json-pretty"));
  f->open_object_section("crush_map");
  crush.dump(f.get());
  f->close_section();
  ofstream fout(out);
  f->flush(fout);
  fout << "\n";
}
