import {
  Table,
  TableIndex,
  type MigrationInterface,
  type QueryRunner,
} from 'typeorm';

export class CurrentAssessments1794185037829 implements MigrationInterface {
  name = 'CurrentAssessments1794185037829';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'airline.connection_current',
        columns: [
          {
            name: 'source_event_id',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'schema_version',
            type: 'varchar',
            isNullable: false,
            length: '16',
          },
          {
            name: 'event_type',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'event_time',
            type: 'timestamptz',
            isNullable: false,
            precision: 3,
          },
          {
            name: 'ingested_at',
            type: 'timestamptz',
            isNullable: false,
            precision: 3,
          },
          {
            name: 'source',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'operating_mode',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'correlation_id',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'simulation_run_id',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'connection_id',
            type: 'uuid',
            isNullable: false,
            isPrimary: true,
            primaryKeyConstraintName: 'connection_current_pkey',
          },
          {
            name: 'itinerary_id',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'revision',
            type: 'bigint',
            isNullable: false,
          },
          {
            name: 'status',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'arrival_at_used',
            type: 'timestamptz',
            isNullable: true,
            precision: 3,
          },
          {
            name: 'arrival_basis',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'boarding_cutoff_at_used',
            type: 'timestamptz',
            isNullable: true,
            precision: 3,
          },
          {
            name: 'cutoff_basis',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'cutoff_policy_version',
            type: 'varchar',
            isNullable: true,
            length: '64',
          },
          {
            name: 'minimum_transfer_minutes',
            type: 'integer',
            isNullable: false,
          },
          {
            name: 'available_transfer_minutes',
            type: 'integer',
            isNullable: true,
          },
          {
            name: 'reason_code',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'trigger_event_id',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'inbound_update_event_id',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'outbound_update_event_id',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'itinerary_event_id',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'assessment_policy_version',
            type: 'varchar',
            isNullable: false,
            length: '64',
          },
          {
            name: 'updated_at',
            type: 'timestamptz',
            isNullable: false,
            precision: 3,
          },
        ],
        checks: [
          {
            name: 'connection_current_revision_check',
            expression: 'revision > 0 AND revision <= 9007199254740991',
          },
          {
            name: 'connection_current_schema_version_check',
            expression: "schema_version = '1.0.0'",
          },
          {
            name: 'connection_current_event_type_check',
            expression: "event_type = 'CONNECTION_ASSESSED'",
          },
        ],
      }),
    );
    await queryRunner.createIndex(
      'airline.connection_current',
      new TableIndex({
        name: 'connection_current_itinerary_idx',
        columnNames: ['itinerary_id'],
      }),
    );
    await queryRunner.createIndex(
      'airline.connection_current',
      new TableIndex({
        name: 'connection_current_updated_idx',
        columnNames: ['updated_at'],
      }),
    );
    // PostgreSQL functions and role grants use QueryRunner SQL inside this migration.
    await queryRunner.query(`CREATE FUNCTION airline.apply_connection_assessment(assessment jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, airline AS $function$
DECLARE affected integer;
BEGIN
  IF jsonb_typeof(assessment) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'Assessment must be an object';
  END IF;
  INSERT INTO airline.connection_current (source_event_id, schema_version, event_type, event_time, ingested_at, source, operating_mode, correlation_id, simulation_run_id, connection_id, itinerary_id, revision, status, arrival_at_used, arrival_basis, boarding_cutoff_at_used, cutoff_basis, cutoff_policy_version, minimum_transfer_minutes, available_transfer_minutes, reason_code, trigger_event_id, inbound_update_event_id, outbound_update_event_id, itinerary_event_id, assessment_policy_version, updated_at)
  SELECT source_event_id, schema_version, event_type, event_time, ingested_at, source, operating_mode, correlation_id, simulation_run_id, connection_id, itinerary_id, revision, status, arrival_at_used, arrival_basis, boarding_cutoff_at_used, cutoff_basis, cutoff_policy_version, minimum_transfer_minutes, available_transfer_minutes, reason_code, trigger_event_id, inbound_update_event_id, outbound_update_event_id, itinerary_event_id, assessment_policy_version, updated_at
  FROM jsonb_populate_record(NULL::airline.connection_current,
    assessment || jsonb_build_object('source_event_id', assessment->'event_id', 'updated_at', clock_timestamp()))
  ON CONFLICT (connection_id) DO UPDATE SET
    source_event_id = EXCLUDED.source_event_id,
    schema_version = EXCLUDED.schema_version,
    event_type = EXCLUDED.event_type,
    event_time = EXCLUDED.event_time,
    ingested_at = EXCLUDED.ingested_at,
    source = EXCLUDED.source,
    operating_mode = EXCLUDED.operating_mode,
    correlation_id = EXCLUDED.correlation_id,
    simulation_run_id = EXCLUDED.simulation_run_id,
    itinerary_id = EXCLUDED.itinerary_id,
    revision = EXCLUDED.revision,
    status = EXCLUDED.status,
    arrival_at_used = EXCLUDED.arrival_at_used,
    arrival_basis = EXCLUDED.arrival_basis,
    boarding_cutoff_at_used = EXCLUDED.boarding_cutoff_at_used,
    cutoff_basis = EXCLUDED.cutoff_basis,
    cutoff_policy_version = EXCLUDED.cutoff_policy_version,
    minimum_transfer_minutes = EXCLUDED.minimum_transfer_minutes,
    available_transfer_minutes = EXCLUDED.available_transfer_minutes,
    reason_code = EXCLUDED.reason_code,
    trigger_event_id = EXCLUDED.trigger_event_id,
    inbound_update_event_id = EXCLUDED.inbound_update_event_id,
    outbound_update_event_id = EXCLUDED.outbound_update_event_id,
    itinerary_event_id = EXCLUDED.itinerary_event_id,
    assessment_policy_version = EXCLUDED.assessment_policy_version,
    updated_at = EXCLUDED.updated_at
  WHERE EXCLUDED.revision > connection_current.revision;
  GET DIAGNOSTICS affected = ROW_COUNT;
  RETURN affected = 1;
END
$function$;
REVOKE ALL ON FUNCTION airline.apply_connection_assessment(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION airline.apply_connection_assessment(jsonb) TO airline_runtime;
GRANT SELECT ON airline.connection_current TO airline_runtime;
`);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      'DROP FUNCTION airline.apply_connection_assessment(jsonb)',
    );
    await queryRunner.dropTable('airline.connection_current');
  }
}
