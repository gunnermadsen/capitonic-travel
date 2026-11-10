import 'reflect-metadata';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { DataSource } from 'typeorm';

let dataSource: DataSource | undefined;
const deadline = setTimeout(() => {
  console.error(
    JSON.stringify({ job: 'database-migrations', event: 'deadline_exceeded' }),
  );
  process.exit(1);
}, 120000);

try {
  const directory = process.env.IDENTITY_DIR ?? '/run/identity';
  dataSource = new DataSource({
    type: 'postgres',
    host: process.env.PGHOST ?? 'postgres',
    port: Number(process.env.PGPORT ?? 5432),
    database: process.env.PGDATABASE ?? 'airline',
    username: process.env.PGUSER ?? 'airline_migrator',
    schema: 'airline',
    ssl: {
      ca: await readFile(`${directory}/ca.crt`),
      cert: await readFile(`${directory}/tls.crt`),
      key: await readFile(`${directory}/tls.key`),
      rejectUnauthorized: true,
    },
    synchronize: false,
    migrationsRun: false,
    migrationsTableName: 'typeorm_migrations',
    migrations: [fileURLToPath(new URL('./migrations/*.js', import.meta.url))],
    migrationsTransactionMode: 'all',
    extra: {
      connectionTimeoutMillis: 5000,
      statement_timeout: 30000,
      lock_timeout: 15000,
    },
    logging: false,
  });
  await dataSource.initialize();
  const applied = await dataSource.runMigrations();
  console.log(
    JSON.stringify({
      job: 'database-migrations',
      event: 'completed',
      applied: applied.map((migration) => migration.name),
    }),
  );
} catch (error) {
  console.error(
    JSON.stringify({
      job: 'database-migrations',
      event: 'failed',
      message: error instanceof Error ? error.message : 'Unknown failure',
    }),
  );
  process.exitCode = 1;
} finally {
  try {
    if (dataSource?.isInitialized) await dataSource.destroy();
  } catch {
    console.error(
      JSON.stringify({
        job: 'database-migrations',
        event: 'disconnect_failed',
      }),
    );
    process.exitCode = 1;
  } finally {
    clearTimeout(deadline);
  }
}
