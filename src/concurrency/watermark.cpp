#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  /**< 初次更新 */
  if (reads_ts_map_.empty()) {
    watermark_ = read_ts;
  }

  /**< 更新map */
  if (reads_ts_map_.count(read_ts)) {
    reads_ts_map_[read_ts]++;
  } else {
    reads_ts_map_[read_ts] = 1;
    reads_ts_set_.insert(read_ts);
  }
  /**< 更新watermark_ */
  watermark_ = std::min(read_ts, watermark_);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  BUSTUB_ENSURE(reads_ts_map_.count(read_ts) > 0, "error remove, maybe txn inconsistent");
  /** 更新map */
  if (!(--reads_ts_map_[read_ts])) {
    reads_ts_map_.erase(read_ts);
    reads_ts_set_.erase(read_ts);
    if (read_ts == watermark_) {
      watermark_ = *reads_ts_set_.begin();
    }
  }
}

}  // namespace bustub
