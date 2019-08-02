#include "kudu/consensus/binlog.h"

#include "kudu/util/status.h"
#include "kudu/consensus/log_cache.h"

namespace kudu {
namespace consensus {

Binlog::Binlog(LogCache* lc) : log_cache_(DCHECK_NOTNULL(lc)) {}

Status Binlog::LookupOpId(int64_t op_index, OpId* op_id) const {
  return log_cache_->LookupOpId(op_index, op_id);
}

}
}
