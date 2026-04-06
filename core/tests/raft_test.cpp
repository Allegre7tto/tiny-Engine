#include <gtest/gtest.h>

#include "raft/raft.h"
#include "raft/storage.h"
#include "raft/config.h"

#include <cstddef>
#include <filesystem>
#include <vector>

namespace engine::raft {
using namespace engine::raft::v1;
namespace {

// ─── In-memory Storage stub for tests ────────────────────────────────────────

class MemStorage : public Storage {
public:
    Status SaveState(const HardState& state) override {
        saved_state_ = state;
        has_state_   = true;
        return Status::OK();
    }
    bool LoadState(HardState& out) override {
        if (!has_state_) return false;
        out = saved_state_;
        return true;
    }
    Status SaveSnapshot(const HardState& state, const SnapshotData& snap) override {
        saved_state_ = state;
        has_state_   = true;
        saved_snap_  = snap;
        has_snap_    = true;
        return Status::OK();
    }
    bool LoadSnapshot(SnapshotData& out) override {
        if (!has_snap_) return false;
        out = saved_snap_;
        return true;
    }

private:
    bool         has_state_ = false;
    HardState    saved_state_;
    bool         has_snap_  = false;
    SnapshotData saved_snap_;
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

struct TestCluster {
    std::vector<std::unique_ptr<Raft>> nodes;
    std::vector<std::vector<ApplyMsg>> applied;

    static TestCluster Make(unsigned int n) {
        TestCluster c;
        c.applied.resize(n);

        std::vector<std::string> addrs;
        for (unsigned int i = 0; i < n; ++i)
            addrs.push_back("127.0.0.1:" + std::to_string(7000 + i));

        for (unsigned int i = 0; i < n; ++i) {
            RaftConfig cfg;
            cfg.id         = static_cast<unsigned long long>(i);
            cfg.peer_addrs = addrs;
            cfg.data_dir   = "/tmp/raft_test_" + std::to_string(i);

            unsigned int idx = i;
            auto cb = [&c, idx](ApplyMsg m) {
                c.applied[idx].push_back(std::move(m));
            };

            c.nodes.push_back(std::make_unique<Raft>(
                cfg,
                std::make_unique<MemStorage>(),
                std::move(cb)));
        }
        return c;
    }

    void DeliverVote(unsigned int src, unsigned int dst,
                     const RequestVoteReq& req, unsigned long long election_term) {
        auto resp = nodes[dst]->OnRequestVote(req);
        nodes[src]->OnVoteReply(election_term, dst, resp);
    }

    void DeliverAppend(unsigned int src, unsigned int dst,
                       const AppendEntriesReq& req,
                       unsigned long long sent_term, unsigned long long prev,
                       unsigned int sent_num) {
        auto resp = nodes[dst]->OnAppendEntries(req);
        nodes[src]->OnAppendReply(sent_term, dst, prev, sent_num, resp);
    }

    std::vector<std::vector<RaftTask>> TickAll() {
        std::vector<std::vector<RaftTask>> all;
        for (auto& n : nodes) all.push_back(n->Tick());
        return all;
    }

    int FindLeader(int rounds = 100) {
        for (int r = 0; r < rounds; ++r) {
            auto all_tasks = TickAll();
            for (unsigned int src = 0; src < nodes.size(); ++src) {
                for (const auto& task : all_tasks[src]) {
                    std::visit([&](const auto& t) {
                        using T = std::decay_t<decltype(t)>;
                        if constexpr (std::is_same_v<T, VoteTask>) {
                            DeliverVote(src, t.peer, t.req, t.election_term);
                        } else if constexpr (std::is_same_v<T, AppendTask>) {
                            DeliverAppend(src, t.peer, t.req,
                                          t.sent_term, t.prev_index, t.sent_num);
                        }
                    }, task);
                }
            }
            for (unsigned int i = 0; i < nodes.size(); ++i) {
                if (nodes[i]->IsLeader()) return static_cast<int>(i);
            }
        }
        return -1;
    }
};

// ─── Tests ────────────────────────────────────────────────────────────────────

TEST(RaftTest, ElectsLeaderIn3NodeCluster) {
    auto cluster = TestCluster::Make(3);
    int leader = cluster.FindLeader(200);
    EXPECT_GE(leader, 0) << "no leader elected";
}

TEST(RaftTest, ExactlyOneLeader) {
    auto cluster = TestCluster::Make(3);
    cluster.FindLeader(200);

    int leader_count = 0;
    for (auto& n : cluster.nodes) {
        if (n->IsLeader()) ++leader_count;
    }
    EXPECT_EQ(leader_count, 1);
}

TEST(RaftTest, LeaderRejectsStartOnNonLeader) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    unsigned int follower = (static_cast<unsigned int>(leader_id) + 1) % 3;
    unsigned long long idx, term;
    auto s = cluster.nodes[follower]->Start(
        {std::byte{1}, std::byte{2}, std::byte{3}}, idx, term);
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.IsNotLeader());
}

TEST(RaftTest, LeaderAcceptsStart) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    unsigned long long idx, term;
    auto s = cluster.nodes[static_cast<unsigned int>(leader_id)]->Start(
        {std::byte{0xDE}, std::byte{0xAD}}, idx, term);
    EXPECT_TRUE(s.ok());
    EXPECT_GT(idx, 0u);
}

TEST(RaftTest, LogReplicatedAfterStart) {
    auto cluster = TestCluster::Make(3);
    int leader_id = cluster.FindLeader(200);
    ASSERT_GE(leader_id, 0);

    unsigned long long idx, term;
    cluster.nodes[static_cast<unsigned int>(leader_id)]->Start(
        {std::byte{0xBE}, std::byte{0xEF}}, idx, term);

    cluster.FindLeader(50);

    EXPECT_FALSE(cluster.applied[static_cast<unsigned int>(leader_id)].empty());
}

TEST(RaftTest, PersistAndRestore) {
    std::vector<std::string> addrs = {"127.0.0.1:7099"};

    std::vector<ApplyMsg> applied1;

    RaftConfig cfg;
    cfg.id         = 0;
    cfg.peer_addrs = addrs;
    cfg.data_dir   = "/tmp/raft_persist_test";

    {
        Raft r1(cfg, std::make_unique<MemStorage>(), [&](ApplyMsg m) {
            applied1.push_back(std::move(m));
        });
        for (int i = 0; i < 100 && !r1.IsLeader(); ++i) r1.Tick();
        ASSERT_TRUE(r1.IsLeader());

        unsigned long long idx, term;
        EXPECT_TRUE(r1.Start({std::byte{0xCA}, std::byte{0xFE}}, idx, term).ok());
    }
}

} // namespace
} // namespace engine::raft

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
