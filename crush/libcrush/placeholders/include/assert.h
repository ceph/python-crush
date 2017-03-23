#include <assert.h>
// make __ASSERT_FUNCTION empty (/usr/include/assert.h makes it a function)
// and make our encoding macros break if it non-empty.
#undef __ASSERT_FUNCTION
#define __ASSERT_FUNCTION
