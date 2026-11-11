# Database migrations

A one-shot TypeScript job. `src/main.ts` connects to Postgres, runs pending TypeORM
migrations from `src/migrations/` (compiled to `dist/migrations/`), closes the
connection and exits. TypeORM records applied migrations in
`airline.typeorm_migrations`; repeat runs skip them. Pending migrations run in one
transaction. Failures exit with status 1. Run one migration job at a time.

Use `<epochMilliseconds>-PascalCaseDescription.ts` migration filenames with
matching TypeORM class names, such as `CurrentAssessments1794185037829`.
No manual registry or SQL migration files are needed. Do not edit applied migrations.

Build from the repository root, then run the existing Compose job:

```sh
npm run build
docker compose -f docker-compose.local.yml run --rm database-migrations
```

The job uses the existing migration identity with certificate and hostname
verification. Defaults are `postgres:5432`, database `airline`, role
`airline_migrator`, and identity directory `/run/identity`. Overrides are `PGHOST`,
`PGPORT`, `PGDATABASE`, `PGUSER`, and `IDENTITY_DIR`. The application schema and roles
are provisioned by infrastructure. This job does not manage Polaris metadata.

The old custom ledger is no longer read or maintained. Its existing database table
is left intact; TypeORM history is the sole migration execution history.
