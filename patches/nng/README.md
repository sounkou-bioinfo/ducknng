# Local NNG patches

These patch files document local divergence from the vendored `third_party/nng/` tree.

## Upstream pin

- Project: `nanomsg/nng`
- Vendored line in this repo: `1.11.x` (`third_party/nng/` currently reports `1.11.0` during configure)

## Patch ledger

### `0001-windows-rtools42-timespec-fallback.patch`

Reason:

- DuckDB extension CI currently drives `windows_amd64_mingw` through the DuckDB reusable workflow's Rtools 42 MinGW environment.
- Vendored NNG's Windows CMake gate rejects that environment because `timespec_get()` is missing there, even though the rest of the Windows API surface needed by `ducknng` is present.
- `ducknng` intends to keep supporting `windows_amd64_mingw`, so the vendored copy carries a minimal Windows clock fallback rather than dropping the target.

Files touched:

- `third_party/nng/src/platform/windows/CMakeLists.txt`
- `third_party/nng/src/platform/windows/win_clock.c`

Behavior:

- relax the fatal Windows configure check so missing `timespec_get()` alone does not abort the build
- use `GetSystemTimeAsFileTime()` as a Windows fallback when `timespec_get()` is unavailable

Refresh command:

```sh
git diff -- third_party/nng/src/platform/windows/CMakeLists.txt \
  third_party/nng/src/platform/windows/win_clock.c \
  > patches/nng/0001-windows-rtools42-timespec-fallback.patch
```
