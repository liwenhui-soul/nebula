/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_DRAINERFLAGS_H_
#define DRAINER_DRAINERFLAGS_H_

#include "common/base/Base.h"

DECLARE_uint32(drainer_batch_size);

DECLARE_uint32(max_concurrent_subdrainertasks);

DECLARE_bool(auto_remove_invalid_drainer_space);

DECLARE_int32(request_to_sync_retry_times);

DECLARE_uint32(drainer_task_run_interval);

#endif  // DRAINER_DRAINERFLAGS_H_
