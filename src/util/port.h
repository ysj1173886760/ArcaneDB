/**
 * @file port.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-04-08
 *
 * @copyright Copyright (c) 2023
 *
 */

namespace arcanedb {
namespace util {

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("isb");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

} // namespace util
} // namespace arcanedb