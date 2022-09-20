#!/bin/bash -xe

# Author(s) - anirbanr-fb
# Borrowed from
# https://kudu.apache.org/docs/installation.html#build_from_source

# We need to specify a specific version of Kerberos so we don't break certain
# non-Kudu components that enforce a specific Kerberos version.
KRB5_VERSION=1.18.2-12.el8.x86_64  # NOTE: only correct on CentOS 8!

# INSTALL ALL DEPENDENCIES
sudo dnf install -y autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
  cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk \
  libtool make openssl-devel patch pkgconfig redhat-lsb-core rsync unzip \
  vim-common which cmake doxygen libcap-devel \
  krb5-server-${KRB5_VERSION} krb5-workstation-${KRB5_VERSION}

# DOWNLOAD AND BUILD THE THIRD-PARTY libraries
# IF THINGS BREAK IN MIDDLE
# wipe out: you can use the big hammer!
#  rm -f thirdparty/{src,installed,build}
#  rerun
./thirdparty/build-if-necessary.sh

mkdir -p build/release
cd build/release
cmake -DCMAKE_BUILD_TYPE=release ../..
make -j20
