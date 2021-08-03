lib_LTLIBRARIES = libzoa.la

sbin_PROGRAMS = zfs_object_agent zoa_test

common_sources = $(wildcard zettacache/src/*.rs zettacache/Cargo.toml zettaobject/src/*.rs zettaobject/Cargo.toml Cargo.lock Cargo.toml)

zfs_object_agent_SOURCES = $(wildcard server/src/*.rs server/Cargo.toml) $(common_sources)

zoa_test_SOURCES = $(wildcard client/src/*.rs client/Cargo.toml) $(common_sources)

libzoa_la_SOURCES = $(wildcard zoa/src/*.rs zoa/Cargo.toml) $(common_sources)

RUSTFLAGS = \
	-L $(abs_top_builddir)/lib/libzfs/.libs \
	-L $(abs_top_builddir)/lib/libnvpair/.libs

.PHONY: \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT) \
	libzoa.la

libzoa.la :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zoa $(RUSTTARGET)
	cp target/@RUSTDIR@/libzoa.so .

zfs_object_agent$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zfs_object_agent $(RUSTTARGET)
	cp target/@RUSTDIR@/zfs_object_agent .

zoa_test$(EXEEXT) :
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --package zoa_test $(RUSTTARGET)
	cp target/@RUSTDIR@/zoa_test .

all: build

build: \
	libzoa.la \
	zfs_object_agent$(EXEEXT) \
	zoa_test$(EXEEXT)

install-exec-hook :
	cp target/@RUSTDIR@/libzoa.so $(DESTDIR)$(libdir)/libzoa.so
