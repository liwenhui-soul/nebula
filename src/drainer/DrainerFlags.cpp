/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerFlags.h"

DEFINE_uint32(drainer_batch_size,
              128,
              "The maximum number of logs in a batch when drainer sync data");

DEFINE_uint32(max_concurrent_subdrainertasks, 10, "The sub tasks could be invoked simultaneously");

DEFINE_bool(auto_remove_invalid_drainer_space,
            false,
            "whether remove data of invalid space when scan");

DEFINE_int32(request_to_sync_retry_times, 3, "Retry times if drainer sync data request failed");

DEFINE_uint32(drainer_task_run_interval, 5, "The running interval of each round of drainer task");
