#pragma once

#include "raft/raft.h"
#include "raft/config.h"

#include "raft.grpc.pb.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace engine::raft {

// ─── RaftNode ─────────────────────────────────────────────────────────────────
//
// Wraps Raft + a background ticker thread + gRPC stubs to all peers.
// Implements RaftInternal::Service so it can be registered directly with gRPC.
//
class RaftNode final : public ::engine::raft::v1::RaftInternal::Service {
public:
    RaftNode(const RaftConfig&             cfg,
             std::unique_ptr<Storage>      storage,
             std::function<void(ApplyMsg)> on_commit);

    ~RaftNode();

    RaftNode(const RaftNode&)            = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // ── Public Raft API ───────────────────────────────────────────────────────
    Status Start(const std::vector<std::byte>& data,
                 unsigned long long& out_index, unsigned long long& out_term);
    bool                 IsLeader() const;
    unsigned long long   Term()     const;
    void TakeSnapshot(unsigned long long index, const std::vector<std::byte>& snapshot);
    bool CondInstallSnapshot(unsigned long long last_term, unsigned long long last_index,
                             const std::vector<std::byte>& snapshot);

    // ── gRPC service (RaftInternal) ───────────────────────────────────────────
    grpc::Status RequestVote(grpc::ServerContext*,
                             const ::engine::raft::v1::RequestVoteReq*,
                             ::engine::raft::v1::RequestVoteResp*)     override;

    grpc::Status AppendEntries(grpc::ServerContext*,
                               const ::engine::raft::v1::AppendEntriesReq*,
                               ::engine::raft::v1::AppendEntriesResp*) override;

    grpc::Status InstallSnapshot(grpc::ServerContext*,
                                 const ::engine::raft::v1::InstallSnapshotReq*,
                                 ::engine::raft::v1::InstallSnapshotResp*) override;

private:
    void DispatchTask(const RaftTask& task);

    mutable std::mutex    mutex_;
    std::unique_ptr<Raft> raft_;

    std::vector<std::unique_ptr<::engine::raft::v1::RaftInternal::Stub>> stubs_;

    std::jthread      ticker_;
    std::atomic<bool> stopped_{false};
};

} // namespace engine::raft
