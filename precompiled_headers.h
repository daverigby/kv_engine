#pragma once

#include <nlohmann/json.hpp>

// Used by most unit test files.
#include <folly/portability/GTest.h>
// Used throughout the codebase
#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>

// Included by collections/vbucket_manifest.h, which in turn included
// by 50+ other files.
// Consider changing collections/vbucket_manifest.h to use pimpl for
// Manifest::map which would avoid the need to include F14Map.h.
#include <folly/container/F14Map.h>

#include <map>
#include <string>
