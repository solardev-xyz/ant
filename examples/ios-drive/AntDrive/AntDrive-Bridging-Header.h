#ifndef ANTDRIVE_BRIDGING_HEADER_H
#define ANTDRIVE_BRIDGING_HEADER_H

/*
 * Bridging header for the AntDrive demo app. Re-exports the hand-written
 * C surface in `crates/ant-ffi/include/ant.h` so Swift can call the
 * download, upload, storage, and account entry points directly.
 *
 * The Xcode project adds `crates/ant-ffi/include` to HEADER_SEARCH_PATHS,
 * so this lookup is by basename.
 */
#import "ant.h"

#endif /* ANTDRIVE_BRIDGING_HEADER_H */
