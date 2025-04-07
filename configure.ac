AC_INIT([gap-redis], [0.1], [rafal.lutowski@ug.edu.pl])

AC_PROG_CC

# Sprawdzenie programu gac
AC_ARG_WITH([gac],
            AS_HELP_STRING([--with-gac=ścieżka], [Ścieżka do programu gac]),
            [GAC_PATH="$withval"],
            [AC_PATH_PROG([GAC_PATH], [gac])])

AS_IF([test -z "$GAC_PATH"],
      [AC_MSG_WARN([Nie znaleziono programu gac w ścieżkach systemowych.])
       AC_MSG_WARN([Możesz spróbować określić ścieżkę używając --with-gac.])],
      [AC_MSG_CHECKING([ścieżka do programu gac])
       AC_MSG_RESULT([$GAC_PATH])])

AC_SUBST([GAC_PATH])

# Sprawdzenie biblioteki hiredis
AC_CHECK_LIB([hiredis], [redisConnect],
             [AC_MSG_NOTICE([Znaleziono bibliotekę hiredis.])
              HAVE_HIREDIS=yes],
             [AC_MSG_ERROR([Nie znaleziono biblioteki hiredis.])
              HAVE_HIREDIS=no])

AC_SUBST([HAVE_HIREDIS])

# Zmienna przechowująca bieżący folder
CURRENT_DIR="$PWD"
AC_SUBST([CURRENT_DIR])

AC_CONFIG_FILES([Makefile redis.g])
AC_OUTPUT
