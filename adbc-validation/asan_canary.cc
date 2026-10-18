// AddressSanitizer positive control for the ADBC validation `rust-asan` run.
//
// scripts/run-adbc-validation.sh compiles this with clang `-fsanitize=address` and runs it
// against the instrumented Rust cdylib before database setup. It loads the test-only
// spanner_adbc_asan_canary symbol, allocates a C++ heap buffer, and asks Rust to write one
// byte past its end.
//
// Both sides must share the compiler-rt ASan runtime and shadow memory so the Rust store sees
// the C++ allocation's poisoned redzone. The script requires a non-zero exit and an ASan
// heap-buffer-overflow report; returning normally means the instrumentation check failed.

#include <dlfcn.h>
#include <cstdio>
#include <cstdlib>

typedef void (*canary_fn)(unsigned char* ptr, size_t len);

int main(int argc, char** argv) {
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s <path-to-libspanner_adbc.so>\n", argv[0]);
    return 2;
  }

  const char* lib_path = argv[1];
  void* handle = dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    std::fprintf(stderr, "canary: dlopen(%s) failed: %s\n", lib_path, dlerror());
    return 2;
  }

  dlerror();  // clear any stale error
  void* sym = dlsym(handle, "spanner_adbc_asan_canary");
  const char* dlsym_err = dlerror();
  if (sym == nullptr || dlsym_err != nullptr) {
    std::fprintf(stderr,
                 "canary: dlsym(spanner_adbc_asan_canary) failed: %s\n"
                 "canary: is the cdylib built with --cfg asan_canary?\n",
                 dlsym_err ? dlsym_err : "symbol not found");
    return 2;
  }
  canary_fn canary = reinterpret_cast<canary_fn>(sym);

  // Allocate in C++ so the Rust access must observe the C++ allocator's ASan redzone.
  const size_t len = 16;
  unsigned char* buf = new unsigned char[len];

  std::fprintf(
      stderr,
      "canary: calling instrumented Rust spanner_adbc_asan_canary to write 1 byte past a "
      "%zu-byte C++ heap buffer\n",
      len);
  std::fflush(stderr);

  // Instrumented Rust writes buf[len] — one past the end. ASan should abort here.
  canary(buf, len);

  // Returning normally means the out-of-bounds write went undetected.
  std::fprintf(
      stderr, "canary: RETURNED without an ASan report — the cdylib is not ASan-armed\n");
  delete[] buf;
  return 0;
}
