/* vim: ft=proto
 * Copyright (c) 2021 vesoft inc. All rights reserved.
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

/*
  AppendLogRequest send log messages from sync listener to drainer server.
  Don't need to consider term, drainer make sure the logId are continuous is enough.
*/
struct AppendLogRequest {
    // last_log_id_to_send is the log id for the last log being sent
    //
    1: common.ClusterID        clusterId;           // Source cluster ID
    2: common.GraphSpaceID     space;               // Graphspace ID
    3: common.PartitionID      part;                // Partition ID
    4: i32                     part_num;            // Partition num
    5: common.LogID            last_log_id_to_send; // To be sent last log id this time
    6: common.LogID            last_log_id_sent;    // LastlogId sent successfully last time
    //
    // In the case of AppendLogRequest, the first log id of this time is equal to
    //      last_log_id_sent + 1
    //
    // All logs in the log_str_list must belong to the same term,
    // which specified by log_term
    //
    7: common.TermID          log_term;

    // log id is in the range [last_log_id_sent + 1, last_log_id_to_send]
    8: list<common.LogEntry>  log_str_list;
    // Whether to clear data
    9: bool                   cleanup_data;
    // sync to the space name of the slave cluster
    10: binary                to_space_name;
    //  Used to indicate whether it is meta data.
    11: bool                  sync_meta;

    12: optional common.PropertyType     space_vid_type;      // Master cluster space vid type
    13: optional i16                     space_vid_len;       // Master cluster space vid len
}

struct AppendLogResponse {
    1: common.ErrorCode    error_code;
    2: common.LogID        last_log_id;
}

service DrainerService {
     AppendLogResponse appendLog(1: AppendLogRequest req);
}
