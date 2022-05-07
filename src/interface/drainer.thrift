/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.drainer
namespace java com.vesoft.nebula.drainer
namespace go nebula.drainer
namespace csharp nebula.drainer
namespace js nebula.drainer
namespace py nebula3.drainer

include "common.thrift"

// Sending log messages from sync listener to drainer server.
// Don't need to consider term, drainer ensures the logId is continuous.
struct AppendLogRequest {
    1: common.ClusterID               clusterId;           // Source cluster ID
    2: common.GraphSpaceID            space;               // Graphspace ID
    3: common.PartitionID             part;                // Partition ID
    4: i32                            part_num;            // Total number of partitions
    5: common.LogID                   last_log_id_to_send; // Last logId to be sent this time
    6: common.LogID                   last_log_id_sent;    // Last logId sent successfully last time
    7: common.TermID                  log_term;            // Log term in log_str_list, all logs in the log_str_list must belong to the same term
    8: list<common.LogEntry>          log_str_list;        // Log id is in the range [last_log_id_sent + 1, last_log_id_to_send].
                                                           // First log id in log_str_list is last_log_id_sent + 1
    9: bool                           need_cleanup;        // Whether to cleanup data, before reveiving snapshot
    10: bool                          is_snapshot;         // Whether to send snapshot data
    11: bool                          snapshot_finished;   // Whether the snapshot is sent
    12: common.LogID                  snapshot_commitLogId;// commitlogId after the snapshot is sent，used for snapshot
    13: binary                        to_space_name;       // The space name of the slave cluster to be sync
    14: bool                          sync_meta;           // Whether is it meta data
    15: optional common.PropertyType  space_vid_type;      // Master cluster space vid type
    16: optional i16                  space_vid_len;       // Master cluster space vid length
}

struct AppendLogResponse {
    1: common.ErrorCode    error_code;
    2: common.LogID        last_matched_log_id;
}

service DrainerService {
     AppendLogResponse appendLog(1: AppendLogRequest req);
}
