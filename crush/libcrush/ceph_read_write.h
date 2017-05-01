#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include "libcrush.h"

  int ceph_read_txt_to_json(const char *in, char **out);
  int ceph_read_binary_to_json(const char *in, char **out);
  int ceph_write(LibCrush *self, const char *path, const char *format, PyObject *info);
#ifdef __cplusplus
}
#endif /* __cplusplus */
