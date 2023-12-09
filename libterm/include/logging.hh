#pragma once
#include "print-stack.hh"
#include <source_location>
#include <cstdio>
#include <unistd.h>
#include <fmt/core.h>

#define DYNAMIC_DEBUG       0

#define COLOR_BLACK         "\033[0;30m"
#define COLOR_RED           "\033[0;31m"
#define COLOR_GREEN         "\033[0;32m"
#define COLOR_YELLOW        "\033[0;33m"
#define COLOR_BLUE          "\033[0;34m"
#define COLOR_MAGENTA       "\033[0;35m"
#define COLOR_CYAN          "\033[0;36m"
#define COLOR_WHITE         "\033[0;37m"
#define COLOR_DEFAULT       "\033[0;39m"

#define COLOR_BOLD_BLACK    "\033[1;30m"
#define COLOR_BOLD_RED      "\033[1;31m"
#define COLOR_BOLD_GREEN    "\033[1;32m"
#define COLOR_BOLD_YELLOW   "\033[1;33m"
#define COLOR_BOLD_BLUE     "\033[1;34m"
#define COLOR_BOLD_MAGENTA  "\033[1;35m"
#define COLOR_BOLD_CYAN     "\033[1;36m"
#define COLOR_BOLD_WHITE    "\033[1;37m"
#define COLOR_BOLD_DEFAULT  "\033[1;39m"

#define PT_RESET            "\033[0m"
#define PT_BOLD             "\033[1m"
#define PT_UNDERLINE        "\033[4m"
#define PT_BLINKING         "\033[5m"
#define PT_INVERSE          "\033[7m"

#define LIBPDP_PREFIX COLOR_CYAN "[libpdp] " PT_RESET
#define pr_libpdp(fmt, args...) printf(LIBPDP_PREFIX fmt "\n", ##args)

#define EXTRACT_FILENAME(PATH) (__builtin_strrchr("/" PATH, '/') + 1)
#define __FILENAME__ EXTRACT_FILENAME(__FILE__)

#define pr(fmt, args...)  pr_libpdp(COLOR_GREEN "%s:%d: %s:" PT_RESET " (%d) " fmt, __FILENAME__, __LINE__, __func__, util::Schedule::thread_id(), ##args)
#define pr_flf(file, line, func, fmt, args...) pr_libpdp(COLOR_GREEN "%s:%d: %s:" PT_RESET " (tid=%d) " fmt, file, line, func, util::Schedule::thread_id(), ##args)

#define pr_info(fmt, args...) pr(fmt, ##args)
#define pr_err(fmt, args...)  pr(COLOR_RED fmt PT_RESET, ##args)
#define pr_warn(fmt, args...) pr(COLOR_MAGENTA fmt PT_RESET, ##args)
#define pr_emph(fmt, args...) pr(COLOR_YELLOW fmt PT_RESET, ##args)
#if DYNAMIC_DEBUG
#define pr_debug(fmt, args...) ({ \
    static bool enable_debug = util::getenv_bool("ENABLE_DEBUG").value_or(false); \
    if (enable_debug) pr(fmt, ##args); \
})
#else
#define pr_debug(fmt, args...)
#endif

#define pr_once(level, format...)	({	\
	static bool __warned = false;			\
    if (!__warned) [[unlikely]] { pr_##level(format); 	\
    __warned = true;}		\
})

#define pr_coro(level, format, args...) pr_##level("coro_id=%d, " format, coro_id, ##args)

// #define ASSERT(...)
#define ASSERT(cond, format, args...)  ({ \
    if (!(cond)) [[unlikely]] { \
    pr_err(format, ##args); \
    exit(EXIT_FAILURE); \
    } \
})
#define ASSERT_PERROR(cond, format, args...) ASSERT(cond, "%s: " format, strerror(errno), ##args)

#define fmt_pr(level, fmt_str, args...) pr_##level("%s", fmt::format(fmt_str, ##args).c_str())
