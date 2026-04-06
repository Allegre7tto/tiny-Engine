#include "raft/node.h"

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

namespace engine::raft {

// ─── Construction / Destruction ───────────────────────────────────────────────

RaftNode::RaftNode(const RaftConfig&             cfg,
                   std::unique_ptr<Storage>      storage,
                   std::function<void(ApplyMsg)> on_commit) {
    raft_ = std::make_unique<Raft>(cfg, std::move(storage), std::move(on_commit));

    for (const auto& addr : cfg.peer_addrs) {
        auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        stubs_.push_back(::engine::raft::v1::RaftInternal::NewStub(std::move(ch)));
    }

    ticker_ = std::jthread([this](std::stop_token st) {
        while (!st.stop_requested()) {
            std::vector<RaftTask> tasks;
            {
                std::lock_guard lock(mutex_);
                tasks = raft_->Tick();
            }
            for (const auto& t : tasks) DispatchTask(t);
            std::this_thread::sleep_for(kTickInterval);
        }
    });
}

RaftNode::~RaftNode() { ticker_.request_stop(); }

// ─── Public API ───────────────────────────────────────────────────────────────

Status RaftNode::Start(const std::vector<std::byte>& data,
                       unsigned long long& out_index, unsigned long long& out_term) {
    std::lock_guard lock(mutex_);
    return raft_->Start(data, out_index, out_term);
}

bool RaftNode::IsLeader() const {
    std::lock_guard lock(mutex_);
    return raft_->IsLeader();
}

unsigned long long RaftNode::Term() const {
    std::lock_guard lock(mutex_);
    return raft_->Term();
}

void RaftNode::TakeSnapshot(unsigned long long index,
                            const std::vector<std::byte>& snapshot) {
    std::lock_guard lock(mutex_);
    raft_->TakeSnapshot(index, snapshot);
}

bool RaftNode::CondInstallSnapshot(unsigned long long last_term,
                                   unsigned long long last_index,
                                   const std::vector<std::byte>& snapshot) {
    std::lock_guard lock(mutex_);
    return raft_->CondInstallSnapshot(last_term, last_index, snapshot);
}

// ─── Task dispatcher ─────────────────────────────────────────────────────────

void RaftNode::DispatchTask(const RaftTask& task) {
    std::visit([this](const auto& t) {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, VoteTask>) {
            unsigned int peer = t.peer;
            unsigned long long et = t.election_term;
            auto req = t.req;
            std::thread([this, peer, et, req = std::move(req)]() mutable {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
                ::engine::raft::v1::RequestVoteResp resp;
                if (!stubs_[peer]->RequestVote(&ctx, req, &resp).ok())
                    return;
                std::lock_guard lock(mutex_);
                raft_->OnVoteReply(et, peer, resp);
            }).detach();

        } else if constexpr (std::is_same_v<T, AppendTask>) {
            unsigned int peer = t.peer;
            unsigned long long st = t.sent_term;
            unsigned long long prev = t.prev_index;
            unsigned int sn = t.sent_num;
            auto req = t.req;
            std::thread([this, peer, st, prev, sn, req = std::move(req)]() mutable {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
                ::engine::raft::v1::AppendEntriesResp resp;
                if (!stubs_[peer]->AppendEntries(&ctx, req, &resp).ok())
                    return;
                std::lock_guard lock(mutex_);
                raft_->OnAppendReply(st, peer, prev, sn, resp);
            }).detach();

        } else if constexpr (std::is_same_v<T, SnapshotTask>) {
            unsigned int peer = t.peer;
            unsigned long long st = t.sent_term;
            unsigned long long li = t.last_index;
            auto req = t.req;
            std::thread([this, peer, st, li, req = std::move(req)]() mutable {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(2000));
                ::engine::raft::v1::InstallSnapshotResp resp;
                if (!stubs_[peer]->InstallSnapshot(&ctx, req, &resp).ok())
                    return;
                std::lock_guard lock(mutex_);
                raft_->OnSnapshotReply(st, peer, li, resp);
            }).detach();
        }
    }, task);
}

// ─── gRPC service handlers ────────────────────────────────────────────────────

grpc::Status RaftNode::RequestVote(grpc::ServerContext*,
        const ::engine::raft::v1::RequestVoteReq* req,
        ::engine::raft::v1::RequestVoteResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnRequestVote(*req);
    return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext*,
        const ::engine::raft::v1::AppendEntriesReq* req,
        ::engine::raft::v1::AppendEntriesResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnAppendEntries(*req);
    return grpc::Status::OK;
}

grpc::Status RaftNode::InstallSnapshot(grpc::ServerContext*,
        const ::engine::raft::v1::InstallSnapshotReq* req,
        ::engine::raft::v1::InstallSnapshotResp* resp) {
    std::lock_guard lock(mutex_);
    *resp = raft_->OnInstallSnapshot(*req);
    return grpc::Status::OK;
}

} // namespace engine::raft
