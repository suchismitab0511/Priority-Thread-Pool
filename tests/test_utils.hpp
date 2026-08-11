#pragma once

#include <cstdio>
#include <cstdlib>

// Test check that survives NDEBUG. Unlike assert(), this is a real runtime
// check compiled into every build type, so a Release or CI build cannot
// silently turn the test suite into a no-op.
#define REQUIRE(cond)                                                        \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "REQUIRE failed: %s\n  at %s:%d\n",         \
                         #cond, __FILE__, __LINE__);                         \
            std::abort();                                                    \
        }                                                                    \
    } while (0)

// Same, but prints the two operands so a flaky concurrency failure tells you
// what the values actually were, not just that they differed.
#define REQUIRE_EQ(actual, expected)                                         \
    do {                                                                     \
        const auto actual_val_ = (actual);                                   \
        const auto expected_val_ = (expected);                               \
        if (!(actual_val_ == expected_val_)) {                               \
            std::fprintf(stderr,                                             \
                         "REQUIRE_EQ failed: %s == %s\n"                     \
                         "  actual:   %lld\n  expected: %lld\n  at %s:%d\n", \
                         #actual, #expected,                                 \
                         static_cast<long long>(actual_val_),                \
                         static_cast<long long>(expected_val_),              \
                         __FILE__, __LINE__);                                \
            std::abort();                                                    \
        }                                                                    \
    } while (0)
