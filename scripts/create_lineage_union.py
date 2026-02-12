"""Create unified lineage table (Lineage_All) aggregating File_name and L0-L5 layers."""
from __future__ import annotations

import sqlite3
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
DB_PATH = WORKSPACE / "Dz3_Analysis.db"

FINAL_TABLE = "Lineage_All"
STAGE_TABLE = "Lineage_Stage"


def build_lineage_table() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {FINAL_TABLE}")
        cur.execute(f"DROP TABLE IF EXISTS {STAGE_TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {STAGE_TABLE} (
                SourceLayer TEXT NOT NULL,
                Path_File TEXT,
                File_name TEXT,
                RootServer TEXT,
                RootDatabase TEXT,
                RootSchema TEXT,
                RootTable TEXT,
                ObjectServer TEXT,
                ObjectDatabase TEXT,
                ObjectSchema TEXT,
                ObjectName TEXT,
                ObjectType TEXT,
                ObjectExtractionLevel TEXT,
                DependencyType TEXT,
                DependencyDatabase TEXT,
                DependencySchema TEXT,
                DependencyName TEXT,
                DependencyExtractionLevel TEXT
            )
            """
        )

        # Layer L0: join back to File_name to keep report metadata.
        cur.execute(
            f"""
            INSERT INTO {STAGE_TABLE} (
                SourceLayer,
                Path_File,
                File_name,
                RootServer,
                RootDatabase,
                RootSchema,
                RootTable,
                ObjectServer,
                ObjectDatabase,
                ObjectSchema,
                ObjectName,
                ObjectType,
                ObjectExtractionLevel,
                DependencyType,
                DependencyDatabase,
                DependencySchema,
                DependencyName,
                DependencyExtractionLevel
            )
            SELECT
                'L0' AS SourceLayer,
                fn."Path_File",
                fn."File_name",
                fn."Server" AS RootServer,
                fn."Database" AS RootDatabase,
                fn."Schema" AS RootSchema,
                fn."Table" AS RootTable,
                NULLIF(TRIM(l0."ObjectServer"), '') AS ObjectServer,
                NULLIF(TRIM(l0."ObjectDatabase"), '') AS ObjectDatabase,
                NULLIF(TRIM(l0."ObjectSchema"), '') AS ObjectSchema,
                NULLIF(TRIM(l0."ObjectName"), '') AS ObjectName,
                NULLIF(TRIM(l0."ObjectType"), '') AS ObjectType,
                CAST(l0."ObjectExtractionLevel" AS TEXT) AS ObjectExtractionLevel,
                NULLIF(TRIM(l0."SourceObjectType"), '') AS DependencyType,
                NULLIF(TRIM(l0."Database"), '') AS DependencyDatabase,
                NULLIF(TRIM(l0."Schema"), '') AS DependencySchema,
                NULLIF(TRIM(l0."Table"), '') AS DependencyName,
                '0' AS DependencyExtractionLevel
                        FROM "L0" AS l0
                        LEFT JOIN "File_name" AS fn
                            ON UPPER(TRIM(fn."Server")) = UPPER(TRIM(l0."Server"))
                         AND UPPER(TRIM(fn."Database")) = UPPER(TRIM(l0."Database"))
                         AND UPPER(TRIM(fn."Schema")) = UPPER(TRIM(l0."Schema"))
                         AND UPPER(TRIM(fn."Table")) = UPPER(TRIM(l0."Table"))
            """
        )

        def insert_dependency_layer(layer: str) -> None:
            cur.execute(
                f"""
                WITH ctx AS (
                    SELECT
                        NULLIF(UPPER(TRIM(DependencyDatabase)), '') AS DependencyDatabaseKey,
                        NULLIF(UPPER(TRIM(DependencySchema)), '') AS DependencySchemaKey,
                        NULLIF(UPPER(TRIM(DependencyName)), '') AS DependencyNameKey,
                        MIN(Path_File) AS Path_File,
                        MIN(File_name) AS File_name,
                        MIN(RootServer) AS RootServer,
                        MIN(RootDatabase) AS RootDatabase,
                        MIN(RootSchema) AS RootSchema,
                        MIN(RootTable) AS RootTable
                    FROM {STAGE_TABLE}
                    WHERE DependencyDatabase IS NOT NULL
                      AND DependencySchema IS NOT NULL
                      AND DependencyName IS NOT NULL
                    GROUP BY
                        NULLIF(UPPER(TRIM(DependencyDatabase)), ''),
                        NULLIF(UPPER(TRIM(DependencySchema)), ''),
                        NULLIF(UPPER(TRIM(DependencyName)), '')
                )
                INSERT INTO {STAGE_TABLE} (
                    SourceLayer,
                    Path_File,
                    File_name,
                    RootServer,
                    RootDatabase,
                    RootSchema,
                    RootTable,
                    ObjectServer,
                    ObjectDatabase,
                    ObjectSchema,
                    ObjectName,
                    ObjectType,
                    ObjectExtractionLevel,
                    DependencyType,
                    DependencyDatabase,
                    DependencySchema,
                    DependencyName,
                    DependencyExtractionLevel
                )
                SELECT
                    '{layer}' AS SourceLayer,
                    ctx.Path_File,
                    ctx.File_name,
                    ctx.RootServer,
                    ctx.RootDatabase,
                    ctx.RootSchema,
                    ctx.RootTable,
                    NULLIF(TRIM(src."ObjectServer"), '') AS ObjectServer,
                    NULLIF(TRIM(src."ObjectDatabase"), '') AS ObjectDatabase,
                    NULLIF(TRIM(src."ObjectSchema"), '') AS ObjectSchema,
                    NULLIF(TRIM(src."ObjectName"), '') AS ObjectName,
                    NULLIF(TRIM(src."ObjectType"), '') AS ObjectType,
                    CAST(src."ObjectExtractionLevel" AS TEXT) AS ObjectExtractionLevel,
                    NULLIF(TRIM(src."DependencyType"), '') AS DependencyType,
                    NULLIF(TRIM(src."DependencyDatabase"), '') AS DependencyDatabase,
                    NULLIF(TRIM(src."DependencySchema"), '') AS DependencySchema,
                    NULLIF(TRIM(src."DependencyName"), '') AS DependencyName,
                    CAST(src."DependencyExtractionLevel" AS TEXT) AS DependencyExtractionLevel
                                FROM "{layer}" AS src
                                LEFT JOIN ctx
                                    ON ctx.DependencyDatabaseKey = NULLIF(UPPER(TRIM(src."ObjectDatabase")), '')
                                 AND ctx.DependencySchemaKey = NULLIF(UPPER(TRIM(src."ObjectSchema")), '')
                                 AND ctx.DependencyNameKey = NULLIF(UPPER(TRIM(src."ObjectName")), '')
                """
            )

        for layer in ["L1", "L2", "L3", "L4", "L5"]:
            insert_dependency_layer(layer)

        cur.execute(
            f"""
            CREATE TABLE {FINAL_TABLE} (
                SourceLayer TEXT NOT NULL,
                Path_File TEXT,
                File_name TEXT,
                RootServer TEXT,
                RootDatabase TEXT,
                RootSchema TEXT,
                RootTable TEXT,
                NodeRole TEXT NOT NULL,
                NodeType TEXT,
                NodeServer TEXT,
                NodeDatabase TEXT,
                NodeSchema TEXT,
                NodeName TEXT,
                NodeExtractionLevel TEXT
            )
            """
        )

        cur.execute(
            f"""
            INSERT INTO {FINAL_TABLE} (
                SourceLayer,
                Path_File,
                File_name,
                RootServer,
                RootDatabase,
                RootSchema,
                RootTable,
                NodeRole,
                NodeType,
                NodeServer,
                NodeDatabase,
                NodeSchema,
                NodeName,
                NodeExtractionLevel
            )
            SELECT
                SourceLayer,
                Path_File,
                File_name,
                RootServer,
                RootDatabase,
                RootSchema,
                RootTable,
                'OBJECT' AS NodeRole,
                NULLIF(TRIM(ObjectType), '') AS NodeType,
                NULLIF(TRIM(ObjectServer), '') AS NodeServer,
                NULLIF(TRIM(ObjectDatabase), '') AS NodeDatabase,
                NULLIF(TRIM(ObjectSchema), '') AS NodeSchema,
                NULLIF(TRIM(ObjectName), '') AS NodeName,
                CAST(ObjectExtractionLevel AS TEXT) AS NodeExtractionLevel
            FROM {STAGE_TABLE}
            WHERE NULLIF(TRIM(ObjectName), '') IS NOT NULL
            """
        )

        cur.execute(
            f"""
            INSERT INTO {FINAL_TABLE} (
                SourceLayer,
                Path_File,
                File_name,
                RootServer,
                RootDatabase,
                RootSchema,
                RootTable,
                NodeRole,
                NodeType,
                NodeServer,
                NodeDatabase,
                NodeSchema,
                NodeName,
                NodeExtractionLevel
            )
            SELECT
                SourceLayer,
                Path_File,
                File_name,
                RootServer,
                RootDatabase,
                RootSchema,
                RootTable,
                'DEPENDENCY' AS NodeRole,
                NULLIF(TRIM(DependencyType), '') AS NodeType,
                NULL AS NodeServer,
                NULLIF(TRIM(DependencyDatabase), '') AS NodeDatabase,
                NULLIF(TRIM(DependencySchema), '') AS NodeSchema,
                NULLIF(TRIM(DependencyName), '') AS NodeName,
                CAST(DependencyExtractionLevel AS TEXT) AS NodeExtractionLevel
            FROM {STAGE_TABLE}
            WHERE NULLIF(TRIM(DependencyName), '') IS NOT NULL
            """
        )

        cur.execute(f"DROP TABLE IF EXISTS {STAGE_TABLE}")

        conn.commit()
        total = cur.execute(f"SELECT COUNT(*) FROM {FINAL_TABLE}").fetchone()[0]
        print(f"Creato {FINAL_TABLE} con {total} righe")
    finally:
        conn.close()


if __name__ == "__main__":
    build_lineage_table()
