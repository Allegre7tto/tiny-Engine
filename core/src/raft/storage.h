#pragma once

#include "raft/log.h"
#include "common/status.h"

#include <cstddef>
#include <string>
#include <vector>

namespace engine::raft {

// ─── Snapshot data bundle ─────────────────────────────────────────────────────
struct SnapshotData {
    unsigned long long           last_index = 0;
    unsigned long long           last_term  = 0;
    std::vector<std::byte>       data;
};

// ─── Persistent hard-state ────────────────────────────────────────────────────
struct HardState {
    unsigned long long   term   = 0;
    unsigned long long   voted  = 0;  // 0 = no vote; otherwise (peer_id + 1)
    unsigned long long   base   = 0;
    unsigned long long   anchor = 0;
    std::vector<LogEntry> log;
};

// ─── Storage abstract interface ───────────────────────────────────────────────
class Storage {
public:
    virtual ~Storage() = default;

    virtual Status SaveState(const HardState& state) = 0;
    virtual bool   LoadState(HardState& out) = 0;
    virtual Status SaveSnapshot(const HardState& state, const SnapshotData& snap) = 0;
    virtual bool   LoadSnapshot(SnapshotData& out) = 0;
};

// ─── File-based Storage ───────────────────────────────────────────────────────
//
// data_dir/raft_state.bin  — proto-encoded HardState
// data_dir/snapshot.bin    — proto-encoded SnapshotData
//
// Framing: [4B len][4B crc32][payload]
//
class FileStorage : public Storage {
public:
    explicit FileStorage(std::string data_dir);

    Status SaveState(const HardState& state)                              override;
    bool   LoadState(HardState& out)                                      override;
    Status SaveSnapshot(const HardState& state, const SnapshotData& snap) override;
    bool   LoadSnapshot(SnapshotData& out)                                override;

private:
    Status WriteFile(const std::string& path, const std::string& payload);
    bool   ReadFile (const std::string& path, std::string& payload);

    std::string data_dir_;
};

} // namespace engine::raft
