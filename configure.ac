AC_INIT([gap-redis], [0.1], [rafal.lutowski@ug.edu.pl])

AC_PROG_CC

# Checking the gac program
AC_ARG_WITH([gac],
             AS_HELP_STRING([--with-gac=path], [Path to the gac program]),
             [GAC_PATH="$withval"],
             [AC_PATH_PROG([GAC_PATH], [gac])])

AS_IF([test -z "$GAC_PATH"],
      [AC_MSG_ERROR([The gac program was not found in the system paths.])
       AC_MSG_ERROR([You can try specifying the path using --with-gac.])],
      [AS_IF([test -f "$GAC_PATH" && test -x "$GAC_PATH"],
             [AC_MSG_CHECKING([path to the gac program])
              AC_MSG_RESULT([$GAC_PATH])],
             [AC_MSG_ERROR([The specified gac path "$GAC_PATH" is not a file or is not executable.])
              AC_MSG_ERROR([Please provide a valid path to the gac executable using --with-gac.])])
])

AC_SUBST([GAC_PATH])

# Checking the hiredis library
AC_CHECK_LIB([hiredis], [redisConnect],
             [AC_MSG_NOTICE([Found the hiredis library.])
              have_hiredis=yes],
             [AC_MSG_ERROR([The hiredis library was not found.])
              have_hiredis=no])

LDFLAGS="-lhiredis $LDFLAGS"
AC_SUBST([LDFLAGS])

# Variable storing the current folder
CURRENT_DIR="$PWD"
AC_SUBST([CURRENT_DIR])

AC_CONFIG_FILES([Makefile redis.g])
AC_OUTPUT
