include $(top_srcdir)/config/Rules.am

DEFAULT_INCLUDES += \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/lib/libspl/include

sbin_PROGRAMS = zfs_ids_to_path

zfs_ids_to_path_SOURCES = \
	zfs_ids_to_path.c

zfs_ids_to_path_LDADD = \
        $(top_builddir)/lib/libzfs/libzfs.la
