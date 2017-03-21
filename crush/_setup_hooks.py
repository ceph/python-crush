import os


def build_pre_hook(cmdobj):

    os.system(""" set -ex
    rm -fr build/tmp
    mkdir -p build/tmp
    ( cd build/tmp ; cmake ../../crush/libcrush )
    cp build/tmp/acconfig.h crush/libcrush
    rm -fr build/tmp
    """)


def build_post_hook(cmdobj):
    os.unlink("crush/libcrush/acconfig.h")
