Review the Queen MQ code in this project's current changes. Language: {{language}}.

1. Call `check` for that language. Go through every item against the code that touches Queen, and against code that should touch Queen and does not: a step that acks and pushes in two calls instead of one transaction, a consumer with no error path.
2. For every SDK call you are not certain of, call `example` for that operation and compare method names and arguments.
3. Where the code branches on Queen errors, call `explain_error` for each code or reason it handles and confirm the branch is right.

Report findings, most damaging first: the trap id (or "api"), file:line, what goes wrong at runtime, the fix. Skip style. If nothing applies, say so in one line.
