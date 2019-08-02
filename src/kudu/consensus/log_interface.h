#pragma once

#include <cstdint>

namespace kudu {

class Status;

namespace consensus {

class OpId;

class LogApi {
  virtual Status LookupOpId(int64_t op_index, OpId* op_id) const = 0;
};

}
}
