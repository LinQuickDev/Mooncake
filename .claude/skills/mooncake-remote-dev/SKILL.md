---
name: mooncake-remote-dev
description: Develop and modify Mooncake source code in the local workspace while using work@mooncake-dev:/home/work/Mooncake only for compilation, tests, and runtime validation. Use for Mooncake implementation, bug fixes, refactors, EmbTable or ShareMapStore changes, build failures, and any task that requires editing code and validating it on the remote development host.
---

# Mooncake Local Development and Remote Build

Treat the local repository as the source-authoring workspace and the remote
repository as the build and validation workspace.

## Fixed Workspaces

- Local source workspace: `/Users/zchuang/CLionProjects/Mooncake`
- Remote build host: `work@mooncake-dev`
- Remote build workspace: `/home/work/Mooncake`
- EmbTable full build script: `/home/work/build_mooncake_store.sh`

Edit source files only in the local workspace. Do not make source-code fixes
directly on the remote host.

## Required Workflow

1. Inspect `git status`, branch, and HEAD in the local workspace.
2. Inspect `git status`, branch, and HEAD in the remote workspace.
3. Reconcile any remote source changes before synchronizing:
   - If the remote worktree is dirty, inspect its diff.
   - Bring valuable remote-only source changes back into the local workspace.
   - Do not overwrite a dirty remote source tree until the differences are
     understood and preserved.
4. Implement all requested code changes locally.
5. Run lightweight local checks that do not depend on the remote environment,
   such as formatting, static inspection, and `git diff --check`.
6. Preview synchronization, then copy only the intended changed source files
   from local to remote. Use `rsync` first; if `rsync` is unavailable or the
   preview/transfer fails, fall back to SSH-based file copying with the same
   reviewed file list.
7. Build the project on `work@mooncake-dev` exclusively through
   `/home/work/build_mooncake_store.sh`; run additional tests from
   `/home/work/Mooncake` when needed. When the user explicitly requests
   remote compilation, synchronize the reviewed local changes first and run
   this wrapper before reporting completion.
8. When a build or test fails, diagnose it remotely but apply the fix locally,
   synchronize again, and rerun validation.
9. Report both the local changes and the exact remote build or test result.

## EmbTable / ShareMapStore Specifics

- The module `mooncake-embtable-store/` is built by the dedicated wrapper
  `/home/work/build_mooncake_store.sh`. Do not invoke `cmake --build`, `make`,
  or another project build command directly for this module.
- For first-phase correctness/security work, focus on:
  - ShareMap lifecycle (Insert/BuildIndex race, duplicate-key reject,
    Import failure handling).
  - Fail-closed routing for remote buckets.
  - Strict RPC wire parsing and checked arithmetic.
  - Metadata validation (table, bucket, ShareMap meta).
- Do not refactor distributed generation, replication/lease strategy, PHF
  algorithm, transport protocol architecture, or benchmark design during the
  first phase.

## Synchronization Rules

- Use `rsync` as the first-choice synchronization tool. Prefer an explicit
  changed-file list or `rsync --files-from`, and run a dry run before the
  actual transfer.
- If `rsync` cannot be used or either the dry run or transfer fails, fall back
  to `scp` over SSH using the same explicit, reviewed file list. Do not broaden
  the synchronization scope during fallback, and verify the remote worktree
  after copying.
- Preserve relative paths when copying files.
- Never synchronize `.git`, build directories, caches, virtual environments,
  generated binaries, or unrelated untracked files.
- Do not use a whole-tree `rsync --delete`.
- Review source-file deletions individually before removing them remotely.
- Keep local and remote branches and baseline commits aligned where practical.
- Treat the local workspace as the canonical source after reconciliation.

Example synchronization shape:

```bash
rsync -azR --dry-run <changed-files...> \
  work@mooncake-dev:/home/work/Mooncake/
rsync -azR <changed-files...> \
  work@mooncake-dev:/home/work/Mooncake/
```

Run the command from the local repository root and replace
`<changed-files...>` with reviewed repository-relative paths.

## Remote Validation

Use the repository's remote build wrapper as the sole project build entrypoint,
including during iteration and final validation:

```bash
ssh work@mooncake-dev '/home/work/build_mooncake_store.sh'
```

Do not invoke `cmake --build`, `make`, or another project build command
directly. The wrapper owns the required configuration and build sequence.
Run narrower tests after the wrapper succeeds when the task requires them.

## Guardrails

- Remote compilation does not authorize direct remote source editing.
- Generated build output may remain remote unless the user asks to retrieve it.
- If local and remote histories diverge or both contain overlapping edits,
  pause synchronization and present the conflict instead of choosing one side
  silently.
- Do not claim validation succeeded without the remote command's exit status
  and relevant output.
