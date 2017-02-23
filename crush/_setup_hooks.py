from distutils.dist import Distribution
import os
import shutil


def crush_sources():
    dist = Distribution()
    dist.parse_config_files()
    dist.parse_command_line()

    options = dist.get_option_dict('extension=crush.libcrush')
    sources = (
        options['sources'][1] +
        options['libcrush_headers'][1] +
        options['libcrush_cmake'][1]
    )

    found = []
    for source in sources.split():
        file = os.path.basename(source)
        if os.path.exists("libcrush/crush/" + file):
            found.append(file)
    return found


def build_pre_hook(cmdobj):

    for source in crush_sources():
        shutil.copy("libcrush/crush/" + source,
                    "crush/libcrush/" + source)
    os.system(""" set -ex
    rm -fr build/tmp
    mkdir -p build/tmp
    ( cd build/tmp ; cmake ../../crush/libcrush )
    cp build/tmp/acconfig.h crush/libcrush
    rm -fr build/tmp
    """)


def build_post_hook(cmdobj):
    for source in crush_sources():
        os.unlink("crush/libcrush/" + source)
    os.unlink("crush/libcrush/acconfig.h")
