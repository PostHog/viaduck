"""Credential scrubbing for log-bound text.

Shared by the destination pool (connect/attach exception messages embed
full conninfo strings, password included) and discovery's skip-and-count
logging (whose detail strings can derive from exception text near secret
handling). Scrub at the boundary so "no secret reaches a log" is a
structural property, not a per-call-site promise.

Regex notes: the quoted-value patterns use the unrolled-loop form
``'[^'\\\\]*(?:(?:\\\\.|'')[^'\\\\]*)*'`` — the naive ``(?:\\\\.|[^'])*`` is
ambiguous (a backslash matches both alternatives) and backtracks
exponentially on unclosed quoted strings, which attacker-influenceable
error text can contain (CodeQL py/redos on the earlier form). The special
alternative covers BOTH quoting dialects: libpq backslash-escapes (\\') and
SQL's ''-doubling — discovery.build_attach_uri SQL-doubles quotes, and a
pre-parse exception (e.g. a DuckDB ParserException) embeds the raw
doubled-quote literal, which a backslash-only pattern terminates halfway
through, leaking the password tail into the log line it was scrubbing.
"""

from __future__ import annotations

import re

_QUOTED = r"'[^'\\]*(?:(?:\\.|'')[^'\\]*)*'"

_CRED_PATTERNS = (
    # libpq kv form incl. quoted values and space-padded '='.
    re.compile(rf"password\s*=\s*(?:{_QUOTED}|\S+)", re.IGNORECASE),
    # Python dict-repr style ('password': '...') from wrapped driver kwargs.
    re.compile(
        r"(['\"]password['\"]\s*:\s*)['\"][^'\"\\]*(?:\\.[^'\"\\]*)*['\"]",
        re.IGNORECASE,
    ),
    # URL userinfo. GREEDY password segment so a password containing '@'
    # scrubs to the LAST '@' (the host separator).
    re.compile(r"://([^/:@\s]+):(\S+)@"),
)


def scrub_credentials(text: str) -> str:
    text = _CRED_PATTERNS[0].sub("password=***", text)
    text = _CRED_PATTERNS[1].sub(r"\1'***'", text)
    return _CRED_PATTERNS[2].sub(r"://\1:***@", text)
