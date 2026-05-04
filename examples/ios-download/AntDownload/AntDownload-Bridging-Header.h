#ifndef ANTDOWNLOAD_BRIDGING_HEADER_H
#define ANTDOWNLOAD_BRIDGING_HEADER_H

/*
 * Bridging header for the SwiftUI smoke-test app. Re-exports the
 * hand-written C surface in `crates/ant-ffi/include/ant.h` so Swift
 * can call `ant_init` / `ant_download` / `ant_free_buffer` /
 * `ant_shutdown` directly.
 *
 * The Xcode project adds `crates/ant-ffi/include` to
 * `HEADER_SEARCH_PATHS`, so this lookup is by basename.
 */
#import "ant.h"

#endif /* ANTDOWNLOAD_BRIDGING_HEADER_H */
