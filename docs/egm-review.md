# Elephant Goldfish review of Sounding state machines

Review date: 2026-11-16. Target: [state machine map](state-machines.md) and the
corresponding local runtime changes.

Eight fresh reviewers completed their assigned checks across two full rounds and
one focused final check. Their findings were resolved through runtime fixes,
regression tests and documentation corrections. Identical metadata reloads retain
their existing column-estimate invalidation behavior, explicitly documented in S8.

## Method

This review applies the [Elephant–Goldfish model](https://github.com/google/elephant-goldfish-model)
to an existing implementation and its maintenance documentation. Three separate
reviewers read the official EGM personas: [design critic](https://github.com/google/elephant-goldfish-model/blob/main/agents/design_critic.md),
[codebase details](https://github.com/google/elephant-goldfish-model/blob/main/agents/codebase_details.md),
and [comprehension](https://github.com/google/elephant-goldfish-model/blob/main/agents/comprehension.md).
Each reviewer ran in a native subagent with `fork_turns: none`, without the parent
conversation or prior review reports. They shared the filesystem and host
instructions. All were read-only; implementation and test execution remained with
the parent agent.

The design and codebase reviewers used a task-specific standard: verify the
inventory, transition ownership, recovery, concurrency, stale-result guards,
documentation accuracy, interface compatibility and supporting tests. This is an
audit of implemented behavior, so it does not impose a prospective feature-spec
template. The comprehension reviewer received only its persona and the target
document. Every reviewer reported its required reads and completion status.

The first round reviewed the 153-line map. After remediation, three new reviewers
examined the revised 182-line map and current source. The second round reopened a
rejection-recovery finding for another trigger and identified overlapping metadata
loads. A final focused check used new design and codebase reviewers after those
corrections. Its two findings were addressed in the final revision. This implementation audit does not claim the separate five-stage EGM
design-specification workflow or review-loop convergence.

## Findings and resolution

All initial findings were accepted for correction. Duplicate design and codebase
findings are combined below. The codebase persona labels direct documentation/code
contradictions as Blocker; the corresponding runtime severity from the design
reviewer was Major.

| ID | Review source and severity | Finding | Resolution and evidence |
| --- | --- | --- | --- |
| S1 | Round 1 Design 1: Major; Codebase 1: Blocker; reopened by both reviewers in round 2 | A rejected local request could hide the saved import and expose the wrong resume targets. The first correction covered lock rejection; round 2 found the equivalent failure after mismatched resume options. | `Job.checkpoint_saved` distinguishes published attempts from rejected submissions. `Manager.snapshot` retains the saved import and reports `submission_error` separately. Tests cover lock rejection followed by owner death, a stale resume after an external import, unchanged checkpoint contents and subsequent successful resume. |
| S2 | Design 2: Major; Codebase 2: Blocker | A cached local success hid an unreadable checkpoint, including failures during the lock-protected reread. | Status now exposes the recovery condition even with a completed local job, without offering stale resume targets. `test_unreadable_checkpoint_is_not_hidden_by_cached_local_success` covers both read paths. Published catalog data and the prior job's recorded outcome are unchanged. |
| S3 | Codebase 3: Blocker under its documentation rubric | The map claimed that all public errors redact details, but CLI operator diagnostics intentionally include the underlying exception. | The transport row now distinguishes HTTP/MCP redaction from CLI diagnostics. Runtime behavior is preserved. |
| S4 | Comprehension 1: Major | Refresh, import, dataset-only harvest and import modes were insufficiently distinguished. | Added definitions of tracked jobs, stable import identity, attempts, full/inventory modes, preview opt-in, and the direct harvest path. |
| S5 | Comprehension 2: Major | Receipts and the preview-to-execution decision lacked enough context to interpret billing safeguards. | Defined receipts, blocking evidence, unknown estimates, explicit opt-in, the byte cap's scope and retained job evidence; linked the existing usage documentation. |
| S6 | Comprehension 3: Nit | “Scope” meant different selections in browser search, recommendation caching and workload scans. | Defined the separate scope keys in the terminology section. |
| S7 | Round 2 Design 2: Major | Two metadata loads in the same snapshot could finish out of order, letting an older response replace newer schema/settings and erase its measurements. | Hydration and column-cost publication now share `catalog.metadata_revision`. Hydration checks the captured contents under the write lock. Tests verify that a delayed load cannot replace changed metadata or its measurements, while a sample attachment does not invalidate metadata work. |
| S8 | Final Codebase: Blocker under its documentation rubric | The revised map incorrectly claimed every hydration invalidated older pending loads; identical republication does not change a content-based revision. | Narrowed the guarantee to changed saved metadata. Deliberately retained the existing policy that an explicit metadata reload clears column measurements, even when the metadata is identical. Added a regression for that behavior; no storage migration or hidden publication identifier was introduced. |
| S9 | Final Design: Major | Comparing attempt timestamps let an older cached result hide a later external import if the host clock moved backward. | The published checkpoint now identifies the authoritative attempt. Local memory can replace it only for the same import ID and attempt start time. Regressions cover externally completed and interrupted checkpoints with earlier timestamps. |

## Verification

- Baseline before changes: 203 passing pytest tests.
- Final revision: 228 passing pytest tests, including all 28 Node browser
  regressions. The suite is offline and does not make live
  BigQuery calls.
- New coverage includes terminal checkpoint persistence, interruption, resumed
  attempt identity and phase, lock rejection, cross-process reconciliation,
  corrupt checkpoints, single-use jobs, terminal-publication ownership, shared
  browser submission gating, accurate stopped progress, resumed catalog reloads,
  stale statistics/navigation responses, and column costs after in-place metadata
  replacement.
- `git diff --check` passed. No database schema, dependency, endpoint or tool
  argument changes were required.

The browser tests use synthetic DOM elements and directly invoke handlers; they
validate request ownership and rendering decisions rather than a live browser's
layout or event dispatch. Live Google permissions, latency and availability were
not evaluated.

## Reviewer completion

| Round | Reviewer | Status | Result |
| --- | --- | --- | --- |
| 1 | Design | Completed | Two Major findings, S1 and S2 |
| 1 | Codebase details | Completed | S1, S2 and S3 |
| 1 | Comprehension | Completed | S4, S5 and S6; complete paraphrase returned |
| 2 | Design | Completed | Reopened S1 for mismatched resume; added S7 |
| 2 | Codebase details | Completed | Reopened S1 for mismatched resume |
| 2 | Comprehension | Completed | No remaining actionable findings; complete paraphrase returned |
| Final check | Design | Completed | S9; resolved by checkpoint attempt identity and regression coverage |
| Final check | Codebase details | Completed | S8; resolved by precise documentation and retained-behavior regression |

## Comprehension check

The second-round comprehension reviewer returned the following paraphrase and no
remaining actionable comprehension or self-containment findings.

### What this proposes

This document records Sounding’s runtime state machines and gives maintainers a
map of their owners, transitions, safeguards, and regression coverage. It endorses
explicit refresh coordination while keeping other lifecycles with the code that
owns their effects. Its maintenance guidance preserves request ownership, atomic
publication, recovery evidence, and explicit consent for billed execution.

### How the existing system works, per this document

Refresh jobs discover and import targets under a refresh lock, publish recovery
checkpoints, and resume unfinished work while preserving the import ID and starting
a new attempt. Browser submission gates and request generations prevent overlapping
submissions and stale responses from replacing current state. Catalog transactions
preserve previous snapshots on failure, while delayed writes verify that their
target metadata remains current. Remote operations distinguish unavailable or
unknown evidence from successful empty results, and workload execution requires
explicit opt-in with a byte cap. Supporting request, pagination, and progress
protocols retain their own local control flow.
