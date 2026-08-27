# Scripts

## check_idr_api_spec.py

Maintainer tool — checks whether Rapid7's published InsightIDR Investigations
v2 OpenAPI spec has changed, and refreshes the local copy in
`rapid7-insightidr-skill/references/` if so.

```bash
make check-idr-spec            # checks only if >7 days since last check
make check-idr-spec FORCE=1    # check right now regardless
```

**This is a dev-time tool only — it is not called by the MCP server, at
startup or otherwise.** The running server's actual behavior comes from the
hardcoded endpoint paths and parameter names in `src/insightidr_manager.py`
/ `src/insightidr_client.py`, not from these reference files. Updating the
JSON reference does not change what the server does; a human still has to
read the diff and update the implementation.

### Run this before changing InsightIDR server behavior

**Before modifying `src/insightidr_manager.py`, `src/insightidr_client.py`,
or adding/changing any InsightIDR endpoint, parameter, or enum — run
`make check-idr-spec FORCE=1` first.** Confirm the local
`rapid7-insightidr-skill/references/investigations-api-v2.json` is current
before using it (or your own memory of it) as the source of truth for the
change. This codebase already shipped one real bug (`status` vs `statuses`
as a query param — see `WHATS_NEW_INSIGHTIDR.md`) that a spec check would
have caught; don't rely on a stale local copy or a remembered assumption
when the authoritative spec is one command away.

If the check reports a change (exit code 2), read the printed added/removed
paths, review `references/investigations-api-v2.md` for accuracy, and only
then proceed with the code change.
