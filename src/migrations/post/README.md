# Post migrations

Put migrations here only when a structural change must occur **after** the pipeline update succeeds.

The runner executes files in filename order and records successful applications in
`<catalog>.<schema>._schema_migrations`.

Convention: one top-level Databricks SQL statement per file. If multiple statements must be
coordinated, wrap them in a `BEGIN ... END` SQL scripting block.
