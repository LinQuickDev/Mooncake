// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Test-only scripted control for the mock URMA provider. These hooks let a
// gtest drive the jetty rebuild state machine (ACK timeout -> drain -> flush
// -> rebuild) without real URMA hardware. They are compiled into mock_urma.cpp
// only and are inert until a test arms them via the setters below.
#ifndef MOCK_URMA_TEST_CTRL_H
#define MOCK_URMA_TEST_CTRL_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Clears every scripted hook and all queued completions. Call from each
// test's SetUp/TearDown so scripts never leak across cases.
void mock_urma_test_reset(void);

// Overrides the completion status for the next completions polled out of any
// JFC. Applies to up to `count` completions, then auto-clears. Pass
// status=URMA_CR_ACK_TIMEOUT_ERR (9) to drive the rebuild path.
void mock_urma_set_next_poll_status(int status, int count);

// Queues a synthetic FLUSH_ERR_DONE fence CQE carrying the given local jetty
// id (user_ctx=0). The next urma_poll_jfc on the JFC that owns that jetty
// returns it, which drives onFlushDone -> rebuildJettyUnlocked.
void mock_urma_enqueue_flush_done(uint32_t jetty_local_id);

// Makes the next urma_flush_jetty call return up to `count` completions with
// status URMA_CR_WR_FLUSH_ERR, drawn from the jetty's outstanding WRs, so the
// rebuild path can deliver them through processWrCompletion.
void mock_urma_set_flush_returns_errors(int count);

// Makes the next urma_create_jetty call return NULL, exercising the
// rebuild-failure -> deferred-delete fallback.
void mock_urma_fail_next_create_jetty(void);

#ifdef __cplusplus
}
#endif

#endif  // MOCK_URMA_TEST_CTRL_H
