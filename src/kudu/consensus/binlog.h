#pragma once

#include <memory>

#include "kudu/consensus/log_interface.h"

namespace kudu {
namespace consensus {

class LogCache;

class Binlog : public LogApi {
  public:
    explicit Binlog(LogCache* lc_);
    Status LookupOpId(int64_t op_index, OpId* op_id) const override;
    Binlog() = delete;
  private:
    LogCache* log_cache_;
};

}
}
