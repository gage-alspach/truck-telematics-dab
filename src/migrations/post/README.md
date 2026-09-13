# Post migrations

Place post-deployment SQL migrations here.

Use post migrations for changes that should happen only after the new bundle version has been deployed, such as backfills, compatibility cleanup, or contraction steps. Files are applied once in filename order and recorded in `_schema_migrations` with phase `post`.
