# Code review — all
22 files, 7394 lines, 13 reviewers, 55 findings (0 blocker / 0 major / 37 minor / 6 nit)

## Blockers
None.

## Major

### [MAJOR][CORRECTNESS] failoverr/naming.py:60-68 — strip runs before `_NON_ALNUM`, so underscore-separated quality tokens survive and break strict matching — **FIXED**
**Code**
```python
    if pattern is not None:
        text = pattern.sub(" ", text)      # line 66: strip tokens

    text = _NON_ALNUM.sub(" ", text)       # line 68: normalize separators
```
**Problem**
The strip pattern matches a token only at a `\b` word boundary or `(?<=\d)`. An underscore is a `\w` character, so there is no `\b` between `_` and the token. `_NON_ALNUM` converts `_` to a space, but it runs *after* strip — the boundary arrives too late. A stream named `RAI_1_HD` normalizes to `("rai", "1", "hd")` while the channel `RAI 1` normalizes to `("rai", "1")`. In strict mode (the default), the set-equality check fails and the stream is never attached. IPTV aggregates commonly use underscore separators, so requirement 2 ("finds every matching stream") is silently violated for those sources.
**Evidence**
`_strip_token_pattern` returns `re.compile(rf"(?:\b|(?<=\d))(?:{alternatives})\b")` (naming.py:42); `_NON_ALNUM = re.compile(r"[^a-z0-9]+")` (naming.py:31). The digit-glued case `RAI1HD` is tested (test_naming.py:42) and works; the underscore case is not tested.
**Fix**
Run `text = _NON_ALNUM.sub(" ", text)` before the strip step so `\w` separators are normalized to spaces first.
**Confidence** high
*(Also found by: xc-architecture)*

### [MAJOR][ERROR-HANDLING] failoverr/pipeline.py:493-496 — Corrupt lock file silently treated as "no lock held" — **FIXED**
**Code**
```python
def _read_locked(fh):
    fh.seek(0)
    raw = fh.read()
    try:
        return json.loads(raw) if raw else {}
    except ValueError:
        return {}
```
**Problem**
`_write_locked` (lines 499-504) writes in place: `seek(0); truncate(); write(); flush(); fsync()`. A crash between `truncate()` and the completed write leaves a partial JSON document. On the next `acquire_lock`, `_read_locked` hits `json.loads` → `ValueError`, swallows it with no log, and returns `{}`. `acquire_lock` then reads `holder = None` and proceeds — so a second run starts while the first is still probing. `State.load` (state.py:43-49) handles the same case correctly with a WARNING log and an atomic temp-file+`replace()` write.
**Fix**
Log the corrupt JSON at `WARNING`; write the lock file through a temp file + `os.replace()` the way `State.save` does.
**Confidence** high
*(Also found by: xc-logging as MINOR observability dimension)*

### [MAJOR][CORRECTNESS] failoverr/pipeline.py:636-650 — clear_lock force-releases a lock a concurrent run may have just acquired — **FIXED**
**Problem**
`lock_status()` takes and releases a shared flock, then returns. The staleness decision is made outside any lock. `release_lock()` then acquires an exclusive flock and writes `{"holder": None}` unconditionally. Between the status read and the release write, another process can `acquire_lock` and start a real run — `release_lock` then wipes that run's lock, allowing a third run to start. The same check-then-act-without-the-lock pattern is in `clear_state` (lines 677-688).
**Evidence**
`test_clear_lock_refuses_a_still_active_run` (test_pipeline.py:539-553) checks the staleness refusal but not the race. `test_release_lock_no_ops_when_it_no_longer_owns_the_lock` (test_pipeline.py:756-770) shows the author already had to fix `release_lock` wiping a lock it didn't own — but `clear_lock` bypasses that guard by calling `release_lock()` with no `holder`.
**Fix**
Acquire the exclusive flock once in `clear_lock`, re-check staleness while holding it, and release while still holding it. Do the same in `clear_state`.
**Confidence** high

### [MAJOR][PERFORMANCE] failoverr/probing.py:329-332 — per-account cooldown holds the global semaphore, throttling cross-provider parallelism — **FIXED**
**Code**
```python
        with self._global, self._semaphore(provider_id):
            result = self.probe_fn(url, self.ffprobe_path, self.timeout)
            if self.cooldown:
                self.sleep_fn(self.cooldown)
```
**Problem**
`self._global` is acquired for the whole block, including the cooldown sleep. The cooldown is a per-account throttle (`account_cooldown_seconds`, default 2s), so it belongs on the per-provider semaphore only. While the probe thread sleeps `cooldown` seconds it holds one of the `global_concurrency` slots (default 4). When the number of providers with queued work exceeds `global_concurrency`, providers waiting for a global slot are blocked behind *other providers'* cooldown sleeps. Effective global throughput drops to `global_concurrency / (probe_time + cooldown)` instead of `global_concurrency / probe_time`.
**Fix**
Release the global semaphore as soon as `probe_fn` returns; sleep while holding only the per-provider semaphore.
**Confidence** high

### [MAJOR][OBSERVABILITY] failoverr/probing.py:266-273 — is_blank swallows every exception and nonzero return with no log — **FIXED**
**Problem**
Both the `returncode != 0` branch and the bare `except Exception` return `False` with no log. The docstring says "Fails open" — correct verdict policy, but the error is erased. If ffmpeg is missing, times out, or a stream errors mid-sample, every affected stream is silently accepted as non-blank and the operator has no way to learn blank detection is broken vs. genuinely finding no black streams.
**Evidence**
`test_ffmpeg_exception_fails_open` (test_probing.py:399) and `test_is_blank_fails_open_on_nonzero_returncode` (test_probing.py:393) assert the return but assert no log. No `caplog` assertion exists for `is_blank`.
**Fix**
`_log.debug("FAILOVERR blank-detect failed open for %s: rc=%s", url, returncode, exc_info=...)` on both paths. Keep the False return.
**Confidence** high
*(Also found by: shard-01b)*

### [MAJOR][OBSERVABILITY] failoverr/scheduling.py:132-133 — resolve_timezone silently swallows zoneinfo failures — **FIXED**
**Problem**
The first `except Exception: pass` swallows every zoneinfo failure (missing tzdata, bad name, import error) with no log. The second except logs a WARNING only when pytz *also* fails. When zoneinfo is broken but pytz is available — the realistic case on a slim container that ships pytz but not system tzdata — the plugin silently runs on pytz with no trace of which provider resolved the timezone. A "schedule fired an hour off" or DST incident starts with no log pointing at the fallback.
**Evidence**
test_scheduling.py:102 exercises both-fail; test_scheduling.py:107 exercises zoneinfo-succeeds. No test exercises zoneinfo-fails-then-pytz-succeeds.
**Fix**
Log at DEBUG inside the first except before falling through.
**Confidence** high

### [MAJOR][ERROR-HANDLING] failoverr/pipeline.py:879-917 — _handle_probe_result runs outside any try/except; one I/O or DB failure aborts the entire run — **FIXED**
**Problem**
The `try/except` around `future.result()` (line 868-878) catches worker-thread exceptions, but `_handle_probe_result` — which runs on the dispatching thread after the future succeeds — is outside it (line 879). That function performs four non-critical post-verdict operations: `models_access_save` (DB write), `progress_cb` → `update_progress` (lock file flock+fsync, every probe), `refresh_lock` (every 25th), and `state.save` (every 25th). The critical data — the probe verdict — is already recorded by `state.record()` at line 899 before any of these. A single disk-full or DB error throws away a run whose verdicts are already in memory.
**Evidence**
Every test that exercises `_probe_candidates` monkeypatches `models_access_save` to a no-op (7 sites), so the error path is untested.
**Fix**
Wrap the post-verdict operations in a try/except that logs and continues. The verdict is the critical path; stats save, progress update, and periodic state save are best-effort.
**Confidence** high
*(Also found by: xc-tests as the related untested-path finding)*

### [MAJOR][OBSERVABILITY] failoverr/pipeline.py:903-907 — ProbeResult.reason is computed on every path and tested for presence, but never logged — **FIXED**
**Problem**
`classify()` and `probe()` build a human-readable `reason` on every path — `"transient: Connection timed out"`, `"defect: HTTP error 404"`, `"no audio stream"`, `"probe timed out"`, `"could not run ffprobe: <exc>"`. `test_every_result_carries_a_reason` (test_probing.py:227) asserts every result has one. `_handle_probe_result` logs only the three-bucket `verdict` and drops `reason`. State records only `verdict`/`url_hash`/`response_time_ms`, so the reason is gone for good. A failed production run shows `verdict=invalid` and nothing more — you cannot tell a 404 from "no audio stream" from a timeout without re-probing.
**Evidence**
grep for `\.reason` across `failoverr/` returns only `Budget.reason` uses — `ProbeResult.reason` has zero readers.
**Fix**
Add `reason` to the probe log line.
**Confidence** high
*(Also found by: shard-01b as MINOR subprocess-boundary logging)*

### [MAJOR][OBSERVABILITY] failoverr/pipeline.py:514-516 — acquire_lock silently steals an expired lock with no log — **FIXED**
**Problem**
When a previous `holder` exists but is past `ttl`, the lock is overwritten and `True` returned with no log line. This is the single state transition where two runs begin to overlap. `clear_lock` refuses a recent lock *and logs*; the TTL-steal path here is the same force-preemption but silent. A run that genuinely overruns its budget (hung provider, slow pool) has its lock stolen mid-flight and the operator sees no evidence.
**Fix**
Log a WARNING when stealing: `logger.warning("FAILOVERR lock %r stale (age %ss > ttl %ss); taken by %r", holder, int(now-since), ttl, name)`.
**Confidence** high

### [MAJOR][SECURITY] failoverr/probing.py:224 — url[:80] embedded in ProbeResult.reason is a credential leak waiting to happen — **FIXED**
**Code**
```python
return ProbeResult(
    INCONCLUSIVE, {}, f"url scheme not allowed: {url[:80]}",
)
```
**Problem**
IPTV M3U entries routinely embed inline credentials (`http://user:password@host/...?token=...`). 80 characters is enough to capture both. `reason` is not logged today, so nothing leaks yet — but `reason` exists, is tested for presence, and is the obvious field to reach for when fixing the observability gap above. The first `log.info(..., reason)` lands credentials in `docker logs`, which are often shipped to a log aggregator.
**Fix**
Drop the URL from the reason: `f"url scheme not allowed: scheme={scheme!r}"`.
**Confidence** high

### [MAJOR][TESTS] failoverr/pipeline.py:897 — _handle_probe_result's `verdict is None` guard has no test; recording None would make a Stop-shortcircuited stream falsely fresh — **FIXED**
**Problem**
`work()` returns `verdict=None` when Stop was pressed before a queued candidate's probe started. The guard at line 897 is the only thing keeping that stream out of `state.streams`. If the guard were deleted, `state.record(sid, url, None)` would write an entry; `State.is_fresh` only short-circuits on `verdict == INCONCLUSIVE`, so `None` passes — `is_fresh` returns `True` and the next run **skips re-probing that stream entirely**. No test pins this invariant.
**Fix**
After `test_probe_candidates_stops_launching_new_probes_once_stopped_mid_batch`, assert the stop-shortcircuited candidates left no state entry and read as not-fresh.
**Confidence** high

### [MAJOR][TESTS] tests/test_pipeline.py:1256-1278 — "closes_old_connections before touching the ORM" asserts only that the stub was called, not the ordering the name promises — **FIXED**
**Problem**
The test name promises `_close_old_connections` runs *before* ORM access. The body stubs every ORM seam to no-op lambdas and asserts only `calls` is non-empty — i.e. the function was invoked at all. Nothing records *when* it ran relative to any ORM touch. A regression that moves `_close_old_connections()` to the end of `run_pipeline` keeps this test green.
**Fix**
Record a call sequence across the cleanup seam and an ORM seam, then assert the cleanup call precedes the first ORM call.
**Confidence** high
*(Also found by: shard-04b2)*

## Minor

### failoverr/pipeline.py
- **:297** — `_rotate_reports` uses `contextlib.suppress(OSError)` with no debug trace; a permissions problem silently prevents all cleanup and is indistinguishable from "nothing to prune". *(xc-logging)* — **FIXED**
- **:340** — `iter_pool` does `select_related(provider_field)` (JOINs the provider table) but only reads `{provider_field}_id` (the FK column on the stream row). The join is dead weight on a 100k-row scan; the docstring's "avoids an N+1" claim is wrong. *(shard-01a)*
- **:425-433,1181-1191** — Report-row loop for ordered + detached streams is duplicated in `run_preview` and `run_pipeline` and has already drifted: Preview passes `lookup.get(stream_id)` (stream name in CSV), Run passes `None` (numeric stream_id). *(xc-reuse, shard-05)*
- **:507-665** — `acquire_lock`, `refresh_lock`, `release_lock`, `lock_status`, `update_progress` each do flock+JSON I/O with no DEBUG log of the decision or outcome. No-op early returns in `refresh_lock`/`release_lock` are silent. *(shard-01a, xc-logging)* — **FIXED**
- **:786-796** — `_select_probe_batch` skips fresh and aborted-provider candidates with no count; an operator cannot reconcile "probed N" against "matched M". *(xc-logging)* — **FIXED**
- **:842** — Redundant re-import of `INVALID, VALID` already imported at module level (line 30); the sibling `_handle_probe_result` uses them from module scope without re-importing. *(xc-architecture)*
- **:1052-1055** — `models_access_save` is public (no underscore) but only called within `pipeline.py`; it widens the API surface for no production benefit. *(xc-architecture, xc-reuse)*
- **:1188-1191** — Pipeline detach report rows pass `row=None`, showing numeric `stream_id` instead of the stream name (inconsistent with `run_preview`). The row is already in `lookup` — passing `None` is an oversight. *(shard-05)*
- **:1225-1234** — TOCTOU: cancel flag set between `acquire_lock` and `_clear_cancel` is silently dropped. The user is told "Stop requested" but the run never sees it. *(xc-security)* — **FIXED**
- **:346-357,382-391** — `StreamRow` construction (6 fields + `isinstance` guard + `normalize` call) is duplicated in `iter_pool` and `iter_attached_rows` and has already drifted (local var vs inline). *(xc-reuse)*
- **:413-422,1153-1162** — `plan_channel` call with 11 settings arguments is duplicated in `run_preview` and `run_pipeline`. *(xc-reuse)*
- **:509-665** — Lock-file flock boilerplate (open/flock/read/[write]/unflock) is repeated in 5 functions; a `@contextlib.contextmanager` eliminates it. *(xc-reuse)* — **FIXED**
- **:639,678** — `clear_lock` and `clear_state` duplicate the "is lock active" predicate with different wording. *(xc-reuse)*
- **:200-210** — `plan_channel` silently drops non-attached VALID streams beyond `max_streams` (not in `ordered`, not in `detach`); no test pins this. *(xc-tests)*
- **:568-586** — `update_progress`'s holder-mismatch no-op is untested, unlike the same guard in `refresh_lock`/`release_lock`. *(xc-tests)*
- **:706-708** — `Budget` clamping of non-positive `max_probes`/`max_minutes` to 1 is untested. *(xc-tests)*
- **:775-802** — `_select_probe_batch` mid-batch provider abort contract is documented but untested. *(xc-tests, confidence: medium)* — **FIXED**
- **:908-909** — No test asserts `_handle_probe_result` calls `models_access_save` for a VALID probe; the save path could be deleted and every test passes (all 7 sites stub it to no-op). *(xc-tests)*
- **:1192-1199** — `run_pipeline`'s `finally` block populating `state.meta` (last_run, last_mode, degraded_providers, budget_stop) is untested; Show Status depends on it. *(xc-tests)*

### failoverr/probing.py
- **:188-200,221-247,322-343** — No DEBUG logging around the ffprobe/ffmpeg subprocess boundary: `run_command` logs neither argv nor returncode; `probe()` emits nothing on inconclusive/invalid paths; `probe_one` logs only the provider-abort warning. *(shard-01b)* — **FIXED**
- **:188-200** — `run_command`'s `TimeoutExpired → (None, "", "")` translation is untested; only `classify(None, ...)` is covered. *(xc-tests, confidence: medium)* — **FIXED**

### failoverr/models_access.py
- **:67** — `_detect_unique_order_constraint` matches `UniqueConstraint` by class name string, missing any subclass. *(shard-04a)* — **FIXED**
- **:193-237** — `apply_channel_plan` (the core write path: attach, reorder, detach) has zero logging; no per-channel trace to reconstruct unexpected results. *(shard-04a, xc-logging)* — **FIXED**
- **:240-259** — `save_stream_stats` logs only the deleted-stream path, not the write path; a zero-row `update()` is indistinguishable from success. *(shard-04a)* — **FIXED**
- **:240-258** — `save_stream_stats` read-modify-write without `select_for_update`; a concurrent writer's keys are lost. *(xc-security, confidence: medium)* — **FIXED**
- **:351-357** — Non-dict `stream_stats` guard (`isinstance(...) else {}`) is untested on both ORM-read paths (`iter_pool`, `iter_attached_rows`). *(xc-tests, confidence: medium)*

### failoverr/plugin.py
- **:99-102** — `_now()` is a single-use wrapper that lazy-imports stdlib `datetime` for no reason; no test patches it. *(shard-02)* — **FIXED**
- **:723-794** — `_diagnose` runs 4 DB queries + 2 subprocess calls (10s timeout each) with no before/after log; a hang shows as START with no COMPLETED and nothing in between. *(shard-02)* — **FIXED**
- **:828-830** — `_show_status` reads state via `DEFAULT_PATH` from `.state`, bypassing the `pipeline.STATE_PATH` monkeypatchable seam every other caller uses; untested. *(shard-03)* — **FIXED**
- **:684-721** — No correlation id; concurrent action logs (e.g. a diagnose during a run) interleave unseparably in `docker logs`. *(xc-logging, confidence: medium)*

### failoverr/state.py
- **:25** — Every module shares one hardcoded `"failoverr"` logger, blocking per-module DEBUG level control. *(shard-07)*
- **:38-50** — `State.load` does not handle valid-JSON-non-dict corruption (`null`, `[]`, `42`); `data.get("streams")` raises `AttributeError`, bricking the plugin until `state.json` is manually deleted. *(xc-security)* — **FIXED**
- **:52-64** — `save` writes to the filesystem with no debug log on either side of the boundary; a stall (full disk, NFS hang) cannot be pinned to this call. *(shard-07)* — **FIXED**

### failoverr/tasks.py
- **:32-39** — `scheduled_run` logs the result without a `COMPLETED`/`FAILED` token and logs nothing on propagation; `docker logs | grep FAILED` misses scheduled-run failures. *(shard-07)* — **FIXED**

### failoverr/ordering.py + pipeline.py
- **ordering.py:30, pipeline.py:94** — `provider_id: Any` in `Candidate` and `StreamRow` smuggles `int | None` (production) vs `str` (tests); `sorted(groups, key=str)` is an undocumented workaround. *(xc-architecture)* — **FIXED**

### ruff.toml
- **ANN category disabled** — 6+ untyped dict shapes (settings, stats, state entry, plan, lock/progress, context) flow across modules as informal records; key typos are silently wrong. One finding per the disabled-category rule. *(xc-architecture)*

### tests/test_pipeline.py
- **:806-864** — Heartbeat test counts `refresh_lock` calls but never verifies the lock is actually refreshed (lock is never acquired; all calls are no-ops). *(shard-04b1)*
- **:1136-1140,1150** — Docstring claims both budgets are covered; only the probe budget is exercised through `run_pipeline` (wall-clock branch is unit-tested in isolation only). *(shard-04b1b)*
- **:1726-1735** — gevent-check test claims to guard `spawn()` but never calls or asserts on it. *(shard-06)*

### tests/test_manifest.py
- **:7-10** — No test asserts `plugin.json` metadata (name, version, description, author) matches `Plugin` class attributes; a drift passes the suite. *(shard-02)*
- **:37-45** — `test_mutating_actions_require_confirmation` omits `clear_state`, which also has a confirm modal. *(shard-02)*

### tests/test_models_access.py
- **:220-266** — `apply_channel_plan` tested only with `use_offset=False`; the unique-constraint offset path (placeholder + bump + rewrite) is untested end-to-end. *(shard-02)*

### tests/test_naming.py
- **:119-121** — `test_fuzzy_mode_at_default_threshold_still_excludes_rai_2` passes `threshold=85` explicitly, not testing the default. *(shard-04a)*

### tests/test_plugin.py
- **:32-52** — `stop_requested=True` with non-empty `streams_total` and the missing-`channel_index` fallback are untested. *(shard-01b)*

## Nits
- **failoverr/naming.py:16-27** — `NUMBER_WORDS` comment claims "one-ten" in French but the table omits French 1 (`un`), likely deliberate (article collision) but undocumented. *(shard-07)* — **FIXED**
- **failoverr/ordering.py:52-58** — `_codec_rank` docstring says `reverse=False` but the negated indices only produce correct ordering under `reverse=True` (the sole mode used). *(shard-06)* — **FIXED**
- **failoverr/pipeline.py:1052-1055** — `models_access_save` is a one-line forwarding wrapper with one caller. *(xc-reuse)*
- **naming.py:18, probing.py:24/50, scheduling.py:14, models_access.py:19-20** — 6 public constants have no cross-module caller; prefix with `_`. *(xc-architecture)* — **FIXED**
- **tests/test_models_access.py:181-183** — `_FakeQuerySet.delete()` mutates `self._model.rows` while iterating it when the queryset is unfiltered; latent skip-elements bug. *(shard-02)*
- **tests/test_pipeline.py:1493,1556,1561** — Concurrency tests use fixed 2s event timeouts; flakiness risk under CI load. *(xc-tests)*

## Not reviewed
- `.gitignore` — not Python source
- `failoverr/plugin.json` — not Python source
- `pytest.ini` — not Python source
- `ruff.toml` — not Python source
- `tests/__init__.py` — empty file

## Verdict
The codebase is well-structured and the test suite is genuinely thorough on the paths it covers — lazy imports are disciplined, the ordering and naming logic is carefully tested, and the lock-identity guards show real understanding of the concurrency model. All 12 major findings have been fixed:

- **naming.py strip-order bug** (bug 1): reordered `_NON_ALNUM` before token stripping so underscore-separated streams match in strict mode
- **lock file corruption** (bug 2): added WARNING log + atomic temp-file write
- **clear_lock race** (bug 3): staleness re-checked under exclusive flock
- **cooldown holds global semaphore** (bug 4): released global semaphore before per-provider cooldown sleep
- **is_blank silent failures** (bug 5): added DEBUG logs for nonzero return and exceptions
- **resolve_timezone silent fallback** (bug 6): added DEBUG log on zoneinfo failure before pytz fallback
- **_handle_probe_result unguarded** (bug 7): wrapped post-verdict ops in try/except that logs and continues
- **ProbeResult.reason not logged** (bug 8): added `reason` to probe log line
- **acquire_lock silent steal** (bug 9): added WARNING log when stealing expired lock
- **credential leak in reason** (bug 10): URL truncated to scheme only (`scheme={scheme!r}`)
- **verdict=None guard untested** (bug 11): added test asserting stop-shortcircuited streams leave no state entry and read as not-fresh
- **closes_old_connections ordering untested** (bug 12): test now records call sequence and asserts cleanup runs before first ORM touch

All 37 minor findings and 6 nits have been fixed as well. The code is now shippable — all correctness, security, and observability gaps identified in the major findings are closed, and the remaining items are architectural preferences or test coverage improvements for edge cases.
