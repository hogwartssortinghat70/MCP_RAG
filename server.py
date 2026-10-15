from __future__ import annotations

import json
import re
import sqlite3
import sys
import csv
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DATABASES = {
    "survey": {
        "path": ROOT / "final_respondents_urban_sensor_aws_strict.db",
        "description": "Raw and linked field survey, urban household, sensor, and AWS database.",
    },
    "hvi": {
        "path": ROOT / "singareni_hvi_v6.sqlite",
        "description": "Reviewed-final Heat Vulnerability Index scores, evidence, codebook, and validation database.",
    },
}

MAX_SAMPLE_LIMIT = 50
DEFAULT_QUERY_LIMIT = 1000
MAX_QUERY_LIMIT = 5000
DEFAULT_EXPORT_DIR = ROOT / "example_exports"
SPATIAL_INVENTORY_PATH = ROOT / "spatial_inventory.json"
SPATIAL_JOIN_MAP_PATH = ROOT / "spatial_join_map.json"
COORDINATE_QUALITY_PATH = ROOT / "coordinate_quality_report.csv"
SPATIAL_READINESS_REPORT_PATH = ROOT / "SPATIAL_READINESS_REPORT.md"
RAG_CHUNKS_PATH = ROOT / "rag_chunks.jsonl"
RAG_DOCUMENT_INVENTORY_PATH = ROOT / "rag_document_inventory.json"
RAG_CHUNKING_PLAN_PATH = ROOT / "rag_chunking_plan.json"
RAG_INGESTION_REPORT_PATH = ROOT / "RAG_INGESTION_REPORT.md"
RAG_SEARCH_README_PATH = ROOT / "RAG_SEARCH_README.md"
QGIS_EXPORT_DIR = ROOT / "qgis_exports"
GENERATED_QGIS_EXPORT_DIR = QGIS_EXPORT_DIR / "generated"
QGIS_AUTOMATION_PLAN_DIR = ROOT / "qgis_automation_plan"
QGIS_AUTOMATED_OUTPUTS_DIR = ROOT / "qgis_automated_outputs"
STANDARD_QGIS_GEOJSONS = {
    "hvi_points": QGIS_EXPORT_DIR / "hvi_points.geojson",
    "urban_household_points": QGIS_EXPORT_DIR / "urban_household_points.geojson",
    "main_respondent_points": QGIS_EXPORT_DIR / "main_respondent_points.geojson",
    "sensor_matched_points": QGIS_EXPORT_DIR / "sensor_matched_points.geojson",
}
_QGIS_APP = None
SUPPORTED_POINT_EXPORT_SOURCES = {
    "hvi.hvi_scores",
    "hvi.hvi_geojson_features",
    "hvi.hvi_high_medium_low_list",
    "hvi.vw_hvi_people",
    "survey.RESPONDENT",
    "survey.URBAN_RESPONDENTS",
    "survey.URBAN_SHAPES",
}
FORBIDDEN_SQL_TOKENS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "reindex",
    "analyze",
    "load_extension",
    "begin",
    "commit",
    "rollback",
}


KNOWN_JOINS = [
    {
        "name": "hvi_scores_to_people_170",
        "left": "hvi.hvi_scores h",
        "right": "survey.PEOPLE_170 p",
        "join_sql": (
            "ON ((lower(h.source_db) = 'main' AND h.person_id = p.main_respondent_id) "
            "OR (lower(h.source_db) = 'urban' AND CAST(h.person_id AS TEXT) = CAST(p.urban_household_id AS TEXT)))"
        ),
        "notes": "Direct h.person_id = p.study_person_id returns zero rows; use source-dependent keys.",
    },
    {
        "name": "people_170_to_sensor_rows",
        "left": "survey.PEOPLE_170 p",
        "right": "survey.SENSOR_AWS_RESPONDENT_ROWS s",
        "join_sql": "ON p.study_person_id = s.study_person_id",
        "notes": "Links study people to AWS/sensor reading rows.",
    },
    {
        "name": "people_170_to_respondent",
        "left": "survey.PEOPLE_170 p",
        "right": "survey.RESPONDENT r",
        "join_sql": "ON p.main_respondent_id = r.RESPONDENT_ID",
        "notes": "Main 29 respondent survey rows.",
    },
    {
        "name": "people_170_to_urban_respondents",
        "left": "survey.PEOPLE_170 p",
        "right": "survey.URBAN_RESPONDENTS u",
        "join_sql": "ON CAST(p.urban_household_id AS TEXT) = CAST(u.household_id AS TEXT)",
        "notes": "Urban household/person survey rows.",
    },
    {
        "name": "hvi_scores_to_indicator_group_scores",
        "left": "hvi.hvi_scores hs",
        "right": "hvi.hvi_indicator_group_scores ig",
        "join_sql": "ON hs.person_key = ig.person_key",
        "notes": "Per-person HVI domain and indicator group scores.",
    },
    {
        "name": "hvi_scores_to_answer_evidence",
        "left": "hvi.hvi_scores hs",
        "right": "hvi.hvi_answer_evidence e",
        "join_sql": "ON hs.person_key = e.person_key",
        "notes": "Answer-level evidence and scoring reasons behind each HVI score.",
    },
]


DATABASE_SUMMARY = {
    "survey": {
        "alias": "survey",
        "description": DATABASES["survey"]["description"],
        "important_tables": [
            "PEOPLE_170",
            "RESPONDENT",
            "FAMILY_MEMBERS",
            "URBAN_RESPONDENTS",
            "URBAN_COOK_TIME",
            "URBAN_ILLNESS",
            "URBAN_SHAPES",
            "SENSOR_AWS_RESPONDENT_ROWS",
            "SENSOR_AWS_RESPONDENT_SUMMARY",
            "SENSOR_AWS_URBAN_SUMMARY",
            "SENSOR_RESPONDENT_MAP",
            "SENSOR_DEPLOYMENT_TIME_MAPPING",
        ],
    },
    "hvi": {
        "alias": "hvi",
        "description": DATABASES["hvi"]["description"],
        "important_tables": [
            "hvi_scores",
            "hvi_answer_evidence",
            "hvi_scoring_codebook",
            "hvi_indicator_group_scores",
            "hvi_validation_metric_summary",
            "hvi_validation_person_outcomes",
            "vw_hvi_people",
            "vw_hvi_person_answer_evidence",
            "vw_hvi_codebook_included",
            "vw_top_vulnerable_people",
        ],
    },
}


TOOL_SCHEMAS = [
    {
        "name": "list_databases",
        "description": "Use at the start of SQL analysis to see available database aliases. Returns alias, description, file name, exists, and readable status without exposing full local paths. Do not use for table/schema details or data values. Recommended next tool: health_check or list_tables.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "health_check",
        "description": "Use to verify the MCP SQL layer is ready before analysis. Returns database readability, table/view counts, and HVI-to-survey/sensor join summary. Do not use for answering substantive research questions. Recommended next tool: get_schema_overview, get_known_join_map, or readonly_query.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_tables",
        "description": "Use to discover tables/views in survey or hvi before writing SQL. Returns names and object types. Do not use for columns, samples, counts, or exact answers. Recommended next tool: get_schema_overview or describe_table.",
        "inputSchema": {
            "type": "object",
            "properties": {"database_alias": {"type": "string", "enum": ["survey", "hvi"]}},
            "required": ["database_alias"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_schema_overview",
        "description": "Use for compact database-wide planning before complex SQL. Returns table/view inventory with row counts, column counts, likely primary keys, and important columns. Do not use as proof of exact analytical answers. Recommended next tool: describe_table, get_column_profile, or get_join_template.",
        "inputSchema": {
            "type": "object",
            "properties": {"database_alias": {"type": "string", "enum": ["survey", "hvi"]}},
            "required": ["database_alias"],
            "additionalProperties": False,
        },
    },
    {
        "name": "describe_table",
        "description": "Use when selecting columns, keys, filters, and metrics for SQL. Returns columns, data types, PK/nullability, indexes, foreign keys, and safe sample values. Do not use for full data extraction or final counts. Recommended next tool: get_column_profile, get_table_sample, validate_sql.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database_alias": {"type": "string", "enum": ["survey", "hvi"]},
                "table_name": {"type": "string"},
            },
            "required": ["database_alias", "table_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_table_sample",
        "description": "Use to inspect example row values and category spelling before SQL. Returns a small bounded sample. Do not use sample rows for exact counts, averages, rankings, or final conclusions. Recommended next tool: get_column_profile or validate_sql.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database_alias": {"type": "string", "enum": ["survey", "hvi"]},
                "table_name": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_SAMPLE_LIMIT, "default": 5},
            },
            "required": ["database_alias", "table_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_row_count",
        "description": "Use for exact total row count of one table/view. Returns a single count. Do not use for filtered counts, grouped summaries, or joined counts; write SQL for those. Recommended next tool: readonly_query for filtered/grouped questions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database_alias": {"type": "string", "enum": ["survey", "hvi"]},
                "table_name": {"type": "string"},
            },
            "required": ["database_alias", "table_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_column_profile",
        "description": "Use to understand one column before filtering, grouping, or thresholding. Returns count, null count, distinct count, min/max where applicable, and top frequent values. Do not use as final evidence for multi-column or joined metrics. Recommended next tool: validate_sql after planning result shape.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database_alias": {"type": "string", "enum": ["survey", "hvi"]},
                "table_name": {"type": "string"},
                "column_name": {"type": "string"},
            },
            "required": ["database_alias", "table_name", "column_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "find_common_columns",
        "description": "Use only for exploratory join discovery between two tables. Returns same/similar column names that may be join candidates. Do not treat results as verified joins; known templates override name similarity. Recommended next tool: get_known_join_map or get_join_template.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database_alias_a": {"type": "string", "enum": ["survey", "hvi"]},
                "table_a": {"type": "string"},
                "database_alias_b": {"type": "string", "enum": ["survey", "hvi"]},
                "table_b": {"type": "string"},
            },
            "required": ["database_alias_a", "table_a", "database_alias_b", "table_b"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_known_join_map",
        "description": "Use before any multi-table SQL, especially HVI/survey/sensor analysis. Returns documented known joins and cautions. Do not use for schema columns or final answers. Recommended next tool: get_join_template, describe_table, or validate_sql.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "get_join_template",
        "description": "Use to retrieve one verified join template before writing SQL. Returns left/right tables, join SQL, and notes. Do not invent alternate joins when a known template exists, especially for hvi_scores_to_people_170. Recommended next tool: validate_sql.",
        "inputSchema": {
            "type": "object",
            "properties": {"join_name": {"type": "string"}},
            "required": ["join_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "validate_sql",
        "description": "Use after drafting SQL and before execution. Returns read-only validity, statement type, forbidden tokens, alias use, problems, and normalized SQL. Do not use to judge analytical correctness; it checks safety, not result equivalence. Recommended next tool: explain_sql_plan for complex SQL or readonly_query.",
        "inputSchema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "explain_sql_plan",
        "description": "Use for complex joins, filters, or slow queries after validate_sql passes. Returns SQLite EXPLAIN QUERY PLAN rows. Do not use for final analytical results. Recommended next tool: readonly_query.",
        "inputSchema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "readonly_query",
        "description": "Use for exact database facts: counts, averages, rankings, grouped summaries, HVI/survey/sensor analysis, and row listings. Returns columns, rows, returned row count, and limit warnings. Do not use for qualitative narrative context or GeoJSON export. Recommended next tool: final answer grounding check or revise SQL if result shape is wrong.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_QUERY_LIMIT, "default": DEFAULT_QUERY_LIMIT},
            },
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_join_verification",
        "description": "Use to confirm known HVI-to-PEOPLE_170 and matched sensor subset counts. Returns fixed join verification metrics. Do not use for arbitrary analysis or custom filters. Recommended next tool: get_join_template or readonly_query.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_spatial_sources",
        "description": "Use to discover coordinate-bearing or spatially relevant sources for mapping/export. Returns source name, type, geometry/coordinate columns, usability, and join flags. Do not use for exact database statistics or qualitative context. Recommended next tool: describe_spatial_source or coordinate_quality_report.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "describe_spatial_source",
        "description": "Use before exporting or mapping one spatial source. Returns CRS assumptions, coordinate/geometry columns, join keys, important attributes, coordinate completeness, status, and notes. Do not use for SQL aggregates or RAG context. Recommended next tool: coordinate_quality_report or export_points_geojson.",
        "inputSchema": {
            "type": "object",
            "properties": {"source_name": {"type": "string"}},
            "required": ["source_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "coordinate_quality_report",
        "description": "Use to assess coordinate completeness and validity before GeoJSON/QGIS export. Returns global or source-filtered coordinate quality rows. Do not use for HVI scores, sensor metrics, or narrative context. Recommended next tool: export_points_geojson if coordinates are usable.",
        "inputSchema": {
            "type": "object",
            "properties": {"source_name": {"type": "string"}},
            "additionalProperties": False,
        },
    },
    {
        "name": "export_points_geojson",
        "description": "Use to create QGIS-ready Point GeoJSON from supported coordinate-bearing sources. Returns feature count and output path, or GeoJSON when include_geojson=true. Do not use for unsupported sources, polygon processing, SQL summaries, or final maps. Recommended next tool: describe_spatial_source or load manually in QGIS.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_name": {"type": "string"},
                "output_path": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1},
                "overwrite": {"type": "boolean", "default": False},
                "include_geojson": {"type": "boolean", "default": False},
            },
            "required": ["source_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "export_sql_points_geojson",
        "description": "Use to prepare a filtered point GeoJSON layer from a safe SELECT/WITH SQL result for a parallel QGIS MCP server. Returns output path, feature count, coordinate columns, property fields, sample features, and skipped-row warnings. Do not use for polygon joins, map rendering, database writes, or unsupported/non-coordinate SQL results. Recommended next tool: load the returned GeoJSON path in a QGIS MCP server.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "latitude_column": {"type": "string"},
                "longitude_column": {"type": "string"},
                "output_name": {"type": "string"},
                "label_column": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": MAX_QUERY_LIMIT, "default": 1000},
            },
            "required": ["sql", "latitude_column", "longitude_column", "output_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "export_sensor_matched_points_geojson",
        "description": "Use to export matched sensor subset points aggregated by study_person_id, resolved_respondent_id, or sensor_unique_id. Returns feature count, output path, and summary metrics in properties. Do not use for all sensor rows, HVI score calculations, or polygon joins. Recommended next tool: coordinate_quality_report or QGIS import.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_by": {"type": "string", "enum": ["study_person_id", "resolved_respondent_id", "sensor_unique_id"], "default": "study_person_id"},
                "output_path": {"type": "string"},
                "overwrite": {"type": "boolean", "default": False},
                "include_geojson": {"type": "boolean", "default": False},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_spatial_join_map",
        "description": "Use to inspect documented spatial join possibilities and readiness. Returns the spatial join map from readiness inspection. Do not use for executing SQL joins or polygon spatial joins. Recommended next tool: describe_spatial_source or SQL get_join_template when table joins are needed.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "qgis_check_environment",
        "description": "Use before any QGIS/PyQGIS automation. Returns whether PyQGIS, processing, QGIS app initialization, prefix path, and headless execution appear available. Do not use for maps or data analysis. Recommended next tool: qgis_create_project only if ok=true, otherwise read qgis://environment.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "qgis_load_geojson_layer",
        "description": "Optional PyQGIS tool to load a supported GeoJSON layer into a QGIS project when PyQGIS is available. Returns layer metadata or a graceful unavailable error. Do not use for SQL summaries or non-GeoJSON data. Recommended next tool: qgis_apply_categorized_style or qgis_apply_graduated_style.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "layer_name": {"type": "string"},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "qgis_create_project",
        "description": "Optional PyQGIS tool to create a .qgz project and optionally load standard GeoJSON layers when PyQGIS is available. Returns project path and loaded layers or a graceful unavailable error. Do not use for database edits. Recommended next tool: qgis_load_geojson_layer or styling tools.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_path": {"type": "string"},
                "layer_paths": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["project_path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "qgis_apply_categorized_style",
        "description": "Optional PyQGIS tool to apply categorized styling to an already loaded layer when PyQGIS is available. Use for fields like hvi_category, place, or source_db. Returns style summary or unavailable error. Do not use for numeric graduated styling. Recommended next tool: qgis_export_map_png.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "layer_name": {"type": "string"},
                "field_name": {"type": "string"},
                "categories": {"type": "object"},
            },
            "required": ["layer_name", "field_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "qgis_apply_graduated_style",
        "description": "Optional PyQGIS tool to apply graduated numeric styling to an already loaded layer when PyQGIS is available. Use for hvi_answer_level_score, avg_sensor_temperature_c, avg_aws_heat_index_c, or max_sensor_temperature_c. Returns class summary or unavailable error. Do not use for categorical fields. Recommended next tool: qgis_export_map_png.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "layer_name": {"type": "string"},
                "field_name": {"type": "string"},
                "classes": {"type": "integer", "minimum": 2, "maximum": 12, "default": 5},
                "mode": {"type": "string", "enum": ["quantile", "equal_interval", "jenks", "pretty"], "default": "quantile"},
            },
            "required": ["layer_name", "field_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "qgis_export_map_png",
        "description": "Optional PyQGIS tool to export a basic map image from a QGIS project when PyQGIS is available. Returns output path and rendered layer count or unavailable error. Do not use to compute numeric summaries. Recommended next tool: qgis_export_map_pdf if a PDF is also needed.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_path": {"type": "string"},
                "output_path": {"type": "string"},
                "title": {"type": "string"},
                "width": {"type": "integer", "minimum": 100, "default": 1600},
                "height": {"type": "integer", "minimum": 100, "default": 1200},
            },
            "required": ["output_path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "qgis_export_map_pdf",
        "description": "Optional PyQGIS tool to export a basic map PDF from a QGIS project when PyQGIS is available. Returns output path and rendered layer count or unavailable error. Do not use to compute spatial joins. Recommended next tool: final answer with CRS/layer limitations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_path": {"type": "string"},
                "output_path": {"type": "string"},
                "title": {"type": "string"},
            },
            "required": ["output_path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_polygon_boundary_sources",
        "description": "Use before polygon joins to find candidate polygon boundary files. Returns .shp, .geojson, .gpkg, and detectable spatial files with geometry type, CRS, feature count where readable, and polygon-join usability. Do not claim polygon aggregation unless a usable polygon/multipolygon boundary is found. Recommended next tool: qgis_polygon_join_points_to_boundary only if usable_for_polygon_join=true.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "qgis_polygon_join_points_to_boundary",
        "description": "Optional PyQGIS tool to summarize point attributes by polygon boundary when PyQGIS and a valid polygon layer are available. Returns output path and join summary or graceful unavailable/missing-boundary error. Do not use without a polygon/multipolygon boundary. Recommended next tool: qgis_export_map_png if join succeeds.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "point_layer_path": {"type": "string"},
                "polygon_layer_path": {"type": "string"},
                "output_path": {"type": "string"},
                "summary_fields": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["point_layer_path", "polygon_layer_path", "output_path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_rag_documents",
        "description": "Use to discover qualitative Word-document corpus coverage. Returns doc_id, file_name, category, topic tags, chunk count, and word count. Do not use for exact database counts, HVI scores, sensor values, or coordinates. Recommended next tool: search_rag_chunks.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "search_rag_chunks",
        "description": "Use for qualitative narrative/document context from Word documents, such as water, housing, livelihood, gender, land tenure, or heat experience. Returns ranked BM25 evidence chunks with chunk IDs, file names, previews, metadata, and scores. Do not use for exact database statistics, HVI scores, sensor values, or coordinates. Recommended next tool: get_rag_chunk for selected chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
                "filters": {
                    "type": "object",
                    "properties": {
                        "file_name": {"type": ["string", "array"], "items": {"type": "string"}},
                        "document_category": {"type": ["string", "array"], "items": {"type": "string"}},
                        "topic_tags": {"type": ["string", "array"], "items": {"type": "string"}},
                        "content_type": {"type": ["string", "array"], "items": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_rag_chunk",
        "description": "Use after search_rag_chunks when a specific evidence chunk is needed in full. Returns full text and metadata for one chunk. Do not use for database facts or unsupported chunk IDs. Recommended next tool: final answer grounding check with chunk citation.",
        "inputSchema": {
            "type": "object",
            "properties": {"chunk_id": {"type": "string"}},
            "required": ["chunk_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "filter_rag_chunks",
        "description": "Use to browse qualitative chunks by file, category, topic tags, or content type without BM25 scoring. Returns matching chunk previews and metadata. Do not use for ranked relevance or exact database statistics. Recommended next tool: search_rag_chunks or get_rag_chunk.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_name": {"type": ["string", "array"], "items": {"type": "string"}},
                "document_category": {"type": ["string", "array"], "items": {"type": "string"}},
                "topic_tags": {"type": ["string", "array"], "items": {"type": "string"}},
                "content_type": {"type": ["string", "array"], "items": {"type": "string"}},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "rag_corpus_summary",
        "description": "Use to understand the qualitative corpus before retrieval. Returns total documents/chunks, content type, topic tag, category distributions, and limitations. Do not use for exact HVI/survey/sensor facts. Recommended next tool: list_rag_documents or search_rag_chunks.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
]


PROMPTS = [
    {
        "name": "complex_sql_analysis",
        "description": "Step-by-step workflow for complex SQL analysis over the survey and HVI databases.",
        "arguments": [],
    },
    {
        "name": "sql_debugging",
        "description": "Workflow for debugging failed SQL by reinspecting schema, aliases, columns, and joins.",
        "arguments": [],
    },
    {
        "name": "answer_from_sql_only",
        "description": "Instruction to answer strictly from returned SQL results.",
        "arguments": [],
    },
    {
        "name": "challenge_sql_analysis",
        "description": "Workflow for hard SQL benchmark questions with result-shape planning and self-checks.",
        "arguments": [],
    },
    {
        "name": "result_shape_self_check",
        "description": "Checklist for verifying SELECT columns, grouping level, metrics, ordering, and joins before execution.",
        "arguments": [],
    },
    {
        "name": "sensor_subset_sql",
        "description": "Guidance for HVI + sensor subset SQL using documented join paths and thresholds.",
        "arguments": [],
    },
    {
        "name": "failure_targeted_sql_refinement",
        "description": "Checklist for Step 7D challenge traps: exact labels, thresholds, definitions, and banding.",
        "arguments": [],
    },
    {
        "name": "spatial_export_planning",
        "description": "Plan safe point GeoJSON exports from inspected spatial sources.",
        "arguments": [],
    },
    {
        "name": "coordinate_quality_check",
        "description": "Checklist for reviewing coordinate completeness and range quality before spatial export.",
        "arguments": [],
    },
    {
        "name": "sensor_point_export_planning",
        "description": "Plan matched sensor point exports by aggregating repeated sensor rows.",
        "arguments": [],
    },
    {
        "name": "prepare_layer_for_qgis_mcp",
        "description": "Plan a safe SQL-derived point GeoJSON export that a separate QGIS MCP server can load and map.",
        "arguments": [],
    },
    {
        "name": "rag_retrieval_planning",
        "description": "Plan local keyword/BM25 retrieval over writing chunks before answering.",
        "arguments": [],
    },
    {
        "name": "qualitative_context_lookup",
        "description": "Guide for retrieving qualitative context chunks by topic, file, and content type.",
        "arguments": [],
    },
    {
        "name": "rag_answer_from_chunks_only",
        "description": "Instruction that final synthesis must use retrieved chunks only and cite chunk IDs and file names.",
        "arguments": [],
    },
    {
        "name": "choose_analysis_layer",
        "description": "Route a user question to SQL, spatial export, RAG search, or a combined workflow before using tools.",
        "arguments": [],
    },
    {
        "name": "plan_sql_result_shape",
        "description": "Plan expected SQL answer type, columns, grouping, metrics, filters, joins, ordering, and grain before SQL generation.",
        "arguments": [],
    },
    {
        "name": "answer_with_sql_spatial_rag",
        "description": "Guide final answers that combine SQL results, spatial tool outputs, and RAG chunks with source attribution.",
        "arguments": [],
    },
    {
        "name": "avoid_common_sql_failures",
        "description": "Checklist for avoiding result-shape, grouping, metric, threshold, alias, HVI, sensor, and RAG/SQL boundary failures.",
        "arguments": [],
    },
    {
        "name": "final_answer_grounding_check",
        "description": "Final self-check that every claim is grounded in SQL results, spatial outputs, or cited RAG chunks.",
        "arguments": [],
    },
    {
        "name": "unsupported_question_redirect",
        "description": "Redirect unsupported prediction, medical outcome, future-disaster, or live-weather questions to safe grounded analyses.",
        "arguments": [],
    },
    {
        "name": "qgis_map_planning",
        "description": "Plan QGIS map automation only after checking PyQGIS availability, CRS assumptions, and layer limitations.",
        "arguments": [],
    },
    {
        "name": "qgis_style_selection",
        "description": "Choose categorized or graduated QGIS styling based on field type and map purpose.",
        "arguments": [],
    },
    {
        "name": "qgis_polygon_join_planning",
        "description": "Plan polygon joins only when a valid polygon boundary layer exists and CRS compatibility is checked.",
        "arguments": [],
    },
]


def db_uri(path: Path) -> str:
    return path.resolve().as_uri() + "?mode=ro"


def connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:", uri=True)
    con.row_factory = sqlite3.Row
    for alias, config in DATABASES.items():
        path = config["path"]
        if not path.exists():
            raise FileNotFoundError(f"Missing configured database for alias {alias}: {path}")
        con.execute(f"ATTACH DATABASE ? AS {quote_identifier(alias)}", (db_uri(path),))
    con.execute("PRAGMA query_only = ON")
    return con


def quote_identifier(name: str) -> str:
    if "\x00" in name:
        raise ValueError("Identifier contains a NUL byte.")
    return '"' + name.replace('"', '""') + '"'


def require_alias(alias: str) -> None:
    if alias not in DATABASES:
        raise ValueError(f"Unknown database alias: {alias}")


def table_exists(con: sqlite3.Connection, alias: str, table_name: str) -> dict[str, Any]:
    require_alias(alias)
    row = con.execute(
        f"SELECT name, type FROM {quote_identifier(alias)}.sqlite_master "
        "WHERE type IN ('table','view') AND name = ?",
        (table_name,),
    ).fetchone()
    if not row:
        raise ValueError(f"Table or view not found: {alias}.{table_name}")
    return dict(row)


def table_columns(con: sqlite3.Connection, alias: str, table_name: str) -> list[dict[str, Any]]:
    return [dict(row) for row in con.execute(f"PRAGMA {quote_identifier(alias)}.table_info({quote_identifier(table_name)})")]


def compact_sample_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value)
    if len(text) > 120:
        return text[:117] + "..."
    return text


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n\r]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return sql


def sql_tokens(sql: str) -> list[str]:
    no_comments = strip_sql_comments(sql)
    scrubbed = []
    quote: str | None = None
    i = 0
    while i < len(no_comments):
        ch = no_comments[i]
        if quote:
            if ch == quote:
                if i + 1 < len(no_comments) and no_comments[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            scrubbed.append(" ")
        else:
            if ch in {"'", '"', "`"}:
                quote = ch
                scrubbed.append(" ")
            else:
                scrubbed.append(ch)
        i += 1
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*", "".join(scrubbed).lower())


def split_statements(sql: str) -> list[str]:
    statements = []
    quote: str | None = None
    start = 0
    i = 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            if ch == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        else:
            if ch in {"'", '"', "`"}:
                quote = ch
            elif ch == ";":
                statements.append(sql[start:i].strip())
                start = i + 1
        i += 1
    tail = sql[start:].strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def validate_sql_text(sql: str) -> dict[str, Any]:
    problems: list[str] = []
    stripped = sql.strip()
    if not stripped:
        problems.append("SQL is empty.")
        return {
            "valid": False,
            "read_only": False,
            "statement_type": None,
            "forbidden_tokens_found": [],
            "uses_allowed_alias": False,
            "problems": problems,
            "normalized_sql": "",
        }

    statements = split_statements(stripped)
    if len(statements) != 1:
        problems.append("Only one SQL statement is allowed.")
    statement = statements[0] if statements else stripped.rstrip(";").strip()

    first_token_match = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)", strip_sql_comments(statement))
    first_token = first_token_match.group(1).lower() if first_token_match else ""
    if first_token not in {"select", "with"}:
        problems.append("Only SELECT or WITH queries are allowed.")

    tokens = sql_tokens(stripped)
    forbidden = sorted(set(tokens) & FORBIDDEN_SQL_TOKENS)
    if forbidden:
        problems.append("Forbidden SQL token(s): " + ", ".join(forbidden))

    uses_allowed_alias = bool(re.search(r"\b(survey|hvi)\s*\.", strip_sql_comments(statement), re.I))
    if not uses_allowed_alias:
        problems.append("Query should reference already attached aliases survey. or hvi.")

    read_only = first_token in {"select", "with"} and not forbidden and len(statements) == 1
    return {
        "valid": not problems,
        "read_only": read_only,
        "statement_type": first_token or None,
        "forbidden_tokens_found": forbidden,
        "uses_allowed_alias": uses_allowed_alias,
        "problems": problems,
        "normalized_sql": statement,
    }


def has_limit(sql: str) -> bool:
    return "limit" in sql_tokens(sql)


def apply_limit(sql: str, limit: int) -> tuple[str, bool, str | None]:
    clean = sql.strip().rstrip(";")
    safe_limit = max(1, min(int(limit), MAX_QUERY_LIMIT))
    if has_limit(clean):
        return (
            f"SELECT * FROM ({clean}) AS _mcp_limited_query LIMIT {safe_limit}",
            True,
            f"Applied outer safety LIMIT {safe_limit}; query also contained its own LIMIT.",
        )
    return (
        f"SELECT * FROM ({clean}) AS _mcp_limited_query LIMIT {safe_limit}",
        True,
        f"Applied maximum LIMIT {safe_limit}.",
    )


def response_json(payload: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}]}


def list_databases() -> dict[str, Any]:
    databases = []
    for alias, config in DATABASES.items():
        path = config["path"]
        readable = False
        if path.exists():
            try:
                with sqlite3.connect(db_uri(path), uri=True) as con:
                    con.execute("SELECT 1").fetchone()
                readable = True
            except sqlite3.Error:
                readable = False
        databases.append(
            {
                "alias": alias,
                "description": config["description"],
                "file_name": path.name,
                "exists": path.exists(),
                "readable": readable,
            }
        )
    return {
        "ok": True,
        "databases": databases,
    }


def health_check() -> dict[str, Any]:
    database_status = []
    for alias, config in DATABASES.items():
        path = config["path"]
        status: dict[str, Any] = {
            "alias": alias,
            "file_name": path.name,
            "exists": path.exists(),
            "readable": False,
            "table_count": None,
            "view_count": None,
            "error": None,
        }
        if path.exists():
            try:
                with sqlite3.connect(db_uri(path), uri=True) as con:
                    con.row_factory = sqlite3.Row
                    counts = con.execute(
                        "SELECT type, COUNT(*) AS c FROM sqlite_master "
                        "WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' GROUP BY type"
                    ).fetchall()
                    by_type = {row["type"]: row["c"] for row in counts}
                    status["readable"] = True
                    status["table_count"] = by_type.get("table", 0)
                    status["view_count"] = by_type.get("view", 0)
            except sqlite3.Error as exc:
                status["error"] = str(exc)
        database_status.append(status)
    join_verification = run_join_verification()
    return {
        "ok": all(db["exists"] and db["readable"] for db in database_status) and join_verification["ok"],
        "databases": database_status,
        "join_verification_summary": {
            name: result["value"] for name, result in join_verification["results"].items()
        },
    }


def list_tables(database_alias: str) -> dict[str, Any]:
    with connect() as con:
        require_alias(database_alias)
        rows = con.execute(
            f"SELECT name, type FROM {quote_identifier(database_alias)}.sqlite_master "
            "WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        objects = []
        for row in rows:
            count = con.execute(
                f"SELECT COUNT(*) AS c FROM {quote_identifier(database_alias)}.{quote_identifier(row['name'])}"
            ).fetchone()["c"]
            objects.append({"name": row["name"], "type": row["type"], "row_count": count})
        return {"ok": True, "database_alias": database_alias, "objects": objects}


IMPORTANT_COLUMN_HINTS = (
    "person",
    "study",
    "source",
    "score",
    "category",
    "domain",
    "indicator",
    "answer",
    "question",
    "risk",
    "household",
    "respondent",
    "sensor",
    "aws",
    "timestamp",
    "date",
    "latitude",
    "longitude",
    "uuid",
    "id",
)


def important_columns(columns: list[dict[str, Any]], max_columns: int = 12) -> list[str]:
    selected = [
        col["name"]
        for col in columns
        if any(hint in col["name"].lower() for hint in IMPORTANT_COLUMN_HINTS)
    ]
    if not selected:
        selected = [col["name"] for col in columns[:max_columns]]
    return selected[:max_columns]


def likely_primary_key(columns: list[dict[str, Any]]) -> list[str]:
    declared = [col["name"] for col in columns if col["pk"]]
    if declared:
        return declared
    for suffix in ("_id", "id", "_key", "key", "uuid"):
        candidates = [col["name"] for col in columns if col["name"].lower().endswith(suffix)]
        if candidates:
            return candidates[:3]
    return []


def get_schema_overview(database_alias: str) -> dict[str, Any]:
    with connect() as con:
        require_alias(database_alias)
        rows = con.execute(
            f"SELECT name, type FROM {quote_identifier(database_alias)}.sqlite_master "
            "WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        inventory = []
        for row in rows:
            columns = table_columns(con, database_alias, row["name"])
            count = con.execute(
                f"SELECT COUNT(*) AS c FROM {quote_identifier(database_alias)}.{quote_identifier(row['name'])}"
            ).fetchone()["c"]
            inventory.append(
                {
                    "name": row["name"],
                    "type": row["type"],
                    "row_count": count,
                    "column_count": len(columns),
                    "likely_primary_key": likely_primary_key(columns),
                    "important_columns": important_columns(columns),
                }
            )
        return {"ok": True, "database_alias": database_alias, "objects": inventory}


def describe_table(database_alias: str, table_name: str) -> dict[str, Any]:
    with connect() as con:
        meta = table_exists(con, database_alias, table_name)
        columns = table_columns(con, database_alias, table_name)
        sample_values = {}
        for col in columns:
            col_name = col["name"]
            rows = con.execute(
                f"SELECT DISTINCT {quote_identifier(col_name)} AS value "
                f"FROM {quote_identifier(database_alias)}.{quote_identifier(table_name)} "
                f"WHERE {quote_identifier(col_name)} IS NOT NULL LIMIT 5"
            ).fetchall()
            sample_values[col_name] = [compact_sample_value(row["value"]) for row in rows]
        indexes = [dict(row) for row in con.execute(f"PRAGMA {quote_identifier(database_alias)}.index_list({quote_identifier(table_name)})")]
        foreign_keys = [dict(row) for row in con.execute(f"PRAGMA {quote_identifier(database_alias)}.foreign_key_list({quote_identifier(table_name)})")]
        return {
            "ok": True,
            "database_alias": database_alias,
            "table_name": table_name,
            "type": meta["type"],
            "columns": [
                {
                    "name": col["name"],
                    "type": col["type"],
                    "primary_key_position": col["pk"],
                    "not_null": bool(col["notnull"]),
                    "sample_values": sample_values.get(col["name"], []),
                }
                for col in columns
            ],
            "indexes": indexes,
            "foreign_keys": foreign_keys,
        }


def get_table_sample(database_alias: str, table_name: str, limit: int = 5) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), MAX_SAMPLE_LIMIT))
    with connect() as con:
        table_exists(con, database_alias, table_name)
        rows = con.execute(
            f"SELECT * FROM {quote_identifier(database_alias)}.{quote_identifier(table_name)} LIMIT ?",
            (safe_limit,),
        ).fetchall()
        return {
            "ok": True,
            "database_alias": database_alias,
            "table_name": table_name,
            "limit": safe_limit,
            "columns": list(rows[0].keys()) if rows else [col["name"] for col in table_columns(con, database_alias, table_name)],
            "rows": [dict(row) for row in rows],
            "row_count_returned": len(rows),
        }


def get_row_count(database_alias: str, table_name: str) -> dict[str, Any]:
    with connect() as con:
        table_exists(con, database_alias, table_name)
        count = con.execute(
            f"SELECT COUNT(*) AS c FROM {quote_identifier(database_alias)}.{quote_identifier(table_name)}"
        ).fetchone()["c"]
        return {"ok": True, "database_alias": database_alias, "table_name": table_name, "row_count": count}


def get_column_profile(database_alias: str, table_name: str, column_name: str) -> dict[str, Any]:
    with connect() as con:
        table_exists(con, database_alias, table_name)
        columns = table_columns(con, database_alias, table_name)
        col = next((c for c in columns if c["name"] == column_name), None)
        if not col:
            raise ValueError(f"Column not found: {database_alias}.{table_name}.{column_name}")
        qcol = quote_identifier(column_name)
        qtable = f"{quote_identifier(database_alias)}.{quote_identifier(table_name)}"
        count = con.execute(f"SELECT COUNT(*) AS c FROM {qtable}").fetchone()["c"]
        null_count = con.execute(f"SELECT COUNT(*) AS c FROM {qtable} WHERE {qcol} IS NULL").fetchone()["c"]
        distinct_count = con.execute(f"SELECT COUNT(DISTINCT {qcol}) AS c FROM {qtable}").fetchone()["c"]
        min_max = con.execute(f"SELECT MIN({qcol}) AS min_value, MAX({qcol}) AS max_value FROM {qtable}").fetchone()
        top_values = con.execute(
            f"SELECT {qcol} AS value, COUNT(*) AS frequency FROM {qtable} "
            f"WHERE {qcol} IS NOT NULL GROUP BY {qcol} ORDER BY frequency DESC, {qcol} LIMIT 10"
        ).fetchall()
        return {
            "ok": True,
            "database_alias": database_alias,
            "table_name": table_name,
            "column_name": column_name,
            "type": col["type"],
            "count": count,
            "null_count": null_count,
            "distinct_count": distinct_count,
            "min_value": compact_sample_value(min_max["min_value"]),
            "max_value": compact_sample_value(min_max["max_value"]),
            "top_values": [
                {"value": compact_sample_value(row["value"]), "frequency": row["frequency"]} for row in top_values
            ],
        }


def normalize_column_name(name: str) -> str:
    lowered = name.lower()
    lowered = re.sub(r"[^a-z0-9]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    for suffix in ("_id", "_uuid", "_key", "_name"):
        if lowered.endswith(suffix):
            return lowered[: -len(suffix)] + suffix
    return lowered


def similarity(a: str, b: str) -> float:
    aa = set(normalize_column_name(a).split("_"))
    bb = set(normalize_column_name(b).split("_"))
    if not aa or not bb:
        return 0.0
    return len(aa & bb) / len(aa | bb)


def find_common_columns(database_alias_a: str, table_a: str, database_alias_b: str, table_b: str) -> dict[str, Any]:
    with connect() as con:
        table_exists(con, database_alias_a, table_a)
        table_exists(con, database_alias_b, table_b)
        cols_a = [c["name"] for c in table_columns(con, database_alias_a, table_a)]
        cols_b = [c["name"] for c in table_columns(con, database_alias_b, table_b)]
        exact = sorted(set(cols_a) & set(cols_b))
        candidates = []
        for a in cols_a:
            for b in cols_b:
                score = 1.0 if a == b else similarity(a, b)
                if score >= 0.5 or (
                    any(k in normalize_column_name(a) for k in ("id", "uuid", "person", "household", "study"))
                    and any(k in normalize_column_name(b) for k in ("id", "uuid", "person", "household", "study"))
                    and score >= 0.25
                ):
                    candidates.append({"left_column": a, "right_column": b, "similarity": round(score, 3)})
        candidates = sorted(candidates, key=lambda row: (-row["similarity"], row["left_column"], row["right_column"]))[:50]
        return {
            "ok": True,
            "left": f"{database_alias_a}.{table_a}",
            "right": f"{database_alias_b}.{table_b}",
            "exact_matches": exact,
            "candidate_matches": candidates,
        }


def get_known_join_map() -> dict[str, Any]:
    return {"ok": True, "known_joins": KNOWN_JOINS}


def get_join_template(join_name: str) -> dict[str, Any]:
    join = next((item for item in KNOWN_JOINS if item["name"] == join_name), None)
    if not join:
        return {
            "ok": False,
            "join_name": join_name,
            "known_join_names": [item["name"] for item in KNOWN_JOINS],
            "error": f"Unknown join template: {join_name}",
        }
    return {"ok": True, "join": join}


def explain_sql_plan(sql: str) -> dict[str, Any]:
    validation = validate_sql_text(sql)
    if not validation["valid"]:
        return {"ok": False, "validation": validation, "plan": []}
    with connect() as con:
        rows = con.execute("EXPLAIN QUERY PLAN " + validation["normalized_sql"]).fetchall()
        return {"ok": True, "validation": validation, "plan": [dict(row) for row in rows]}


def readonly_query(sql: str, limit: int = DEFAULT_QUERY_LIMIT) -> dict[str, Any]:
    validation = validate_sql_text(sql)
    if not validation["valid"]:
        return {"ok": False, "validation": validation, "columns": [], "rows": [], "row_count_returned": 0}
    limited_sql, limit_applied, warning = apply_limit(validation["normalized_sql"], limit)
    with connect() as con:
        cur = con.execute(limited_sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return {
            "ok": True,
            "validation": validation,
            "executed_sql": limited_sql,
            "columns": columns,
            "rows": [dict(row) for row in rows],
            "row_count_returned": len(rows),
            "warning": warning if limit_applied else None,
        }


def run_join_verification() -> dict[str, Any]:
    join_sql = (
        "((lower(h.source_db) = 'main' AND h.person_id = p.main_respondent_id) "
        "OR (lower(h.source_db) = 'urban' AND CAST(h.person_id AS TEXT) = CAST(p.urban_household_id AS TEXT)))"
    )
    queries = {
        "total_hvi_people_matched_to_people_170": (
            f"SELECT COUNT(*) AS value FROM hvi.hvi_scores h JOIN survey.PEOPLE_170 p ON {join_sql}"
        ),
        "main_hvi_people_matched": (
            f"SELECT COUNT(*) AS value FROM hvi.hvi_scores h JOIN survey.PEOPLE_170 p ON {join_sql} "
            "WHERE lower(h.source_db) = 'main'"
        ),
        "urban_hvi_people_matched": (
            f"SELECT COUNT(*) AS value FROM hvi.hvi_scores h JOIN survey.PEOPLE_170 p ON {join_sql} "
            "WHERE lower(h.source_db) = 'urban'"
        ),
        "hvi_people_with_sensor_rows": (
            f"SELECT COUNT(DISTINCT h.person_id) AS value FROM hvi.hvi_scores h "
            f"JOIN survey.PEOPLE_170 p ON {join_sql} "
            "JOIN survey.SENSOR_AWS_RESPONDENT_ROWS s ON p.study_person_id = s.study_person_id"
        ),
        "total_matched_sensor_rows": (
            f"SELECT COUNT(*) AS value FROM hvi.hvi_scores h "
            f"JOIN survey.PEOPLE_170 p ON {join_sql} "
            "JOIN survey.SENSOR_AWS_RESPONDENT_ROWS s ON p.study_person_id = s.study_person_id"
        ),
    }
    with connect() as con:
        results = {
            name: {"sql": sql, "value": con.execute(sql).fetchone()["value"]}
            for name, sql in queries.items()
        }
    return {"ok": True, "results": results}


def load_spatial_inventory() -> dict[str, Any]:
    if not SPATIAL_INVENTORY_PATH.exists():
        raise FileNotFoundError(f"Missing spatial inventory: {SPATIAL_INVENTORY_PATH.name}")
    return json.loads(SPATIAL_INVENTORY_PATH.read_text(encoding="utf-8"))


def load_spatial_join_map() -> dict[str, Any]:
    if not SPATIAL_JOIN_MAP_PATH.exists():
        raise FileNotFoundError(f"Missing spatial join map: {SPATIAL_JOIN_MAP_PATH.name}")
    return json.loads(SPATIAL_JOIN_MAP_PATH.read_text(encoding="utf-8"))


def load_coordinate_quality_rows() -> list[dict[str, Any]]:
    if not COORDINATE_QUALITY_PATH.exists():
        raise FileNotFoundError(f"Missing coordinate quality report: {COORDINATE_QUALITY_PATH.name}")
    with COORDINATE_QUALITY_PATH.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def find_spatial_source(source_name: str) -> dict[str, Any]:
    inventory = load_spatial_inventory()
    source = next((item for item in inventory.get("spatial_sources", []) if item.get("source_name") == source_name), None)
    if not source:
        known = [item.get("source_name") for item in inventory.get("spatial_sources", [])]
        raise ValueError(f"Unknown spatial source: {source_name}. Known sources: {', '.join(known)}")
    return source


def list_spatial_sources() -> dict[str, Any]:
    inventory = load_spatial_inventory()
    fields = [
        "source_name",
        "source_type",
        "geometry_type",
        "feature_or_row_count",
        "latitude_column",
        "longitude_column",
        "geometry_column",
        "usability_status",
        "can_join_to_hvi",
        "can_join_to_people_170",
        "can_join_to_sensor_data",
    ]
    return {
        "ok": True,
        "sources": [
            {field: source.get(field) for field in fields}
            for source in inventory.get("spatial_sources", [])
        ],
    }


def describe_spatial_source(source_name: str) -> dict[str, Any]:
    source = find_spatial_source(source_name)
    quality = [row for row in load_coordinate_quality_rows() if row.get("source_name") == source_name]
    return {
        "ok": True,
        "source_name": source_name,
        "crs_assumption": source.get("crs"),
        "coordinate_columns": {
            "latitude": source.get("latitude_column"),
            "longitude": source.get("longitude_column"),
        },
        "geometry_column": source.get("geometry_column"),
        "geometry_columns_all": source.get("geometry_columns_all", []),
        "join_keys": source.get("join_keys", []),
        "important_attributes": source.get("important_attributes", []),
        "coordinate_completeness": quality[0] if quality else source.get("coordinate_quality"),
        "usability_status": source.get("usability_status"),
        "notes": source.get("notes", []),
        "can_join_to_hvi": source.get("can_join_to_hvi"),
        "can_join_to_people_170": source.get("can_join_to_people_170"),
        "can_join_to_sensor_data": source.get("can_join_to_sensor_data"),
    }


def coordinate_quality_report(source_name: str | None = None) -> dict[str, Any]:
    rows = load_coordinate_quality_rows()
    if source_name:
        rows = [row for row in rows if row.get("source_name") == source_name]
        if not rows:
            raise ValueError(f"No coordinate quality row found for source: {source_name}")
    return {"ok": True, "source_name": source_name, "rows": rows, "row_count": len(rows)}


def parse_source_name(source_name: str) -> tuple[str, str]:
    if "." not in source_name:
        raise ValueError(f"Spatial table sources must be database-qualified, e.g. hvi.hvi_scores: {source_name}")
    alias, table = source_name.split(".", 1)
    require_alias(alias)
    return alias, table


def safe_export_path(output_path: str | None, default_file_name: str, overwrite: bool) -> Path:
    if output_path:
        candidate = Path(output_path)
        if not candidate.is_absolute():
            candidate = ROOT / candidate
    else:
        candidate = DEFAULT_EXPORT_DIR / default_file_name
    candidate = candidate.resolve()
    root = ROOT.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("output_path must stay inside the project folder.") from exc
    if candidate.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without overwrite=true: {candidate}")
    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate


def source_default_filename(source_name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", source_name).replace(".", "_")
    return f"{safe}.geojson"


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if not (-90 <= number <= 180):
        return None
    return number


def parse_json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def geojson_feature(lon: float, lat: float, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": properties,
    }


def feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": features,
    }


def write_or_return_geojson(
    collection: dict[str, Any],
    output_path: str | None,
    default_file_name: str,
    overwrite: bool,
    include_geojson: bool = False,
) -> dict[str, Any]:
    if output_path is None:
        path = safe_export_path(None, default_file_name, overwrite)
    else:
        path = safe_export_path(output_path, default_file_name, overwrite)
    path.write_text(json.dumps(collection, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return {
        "geojson": collection if include_geojson else None,
        "output_path": str(path),
    }


def is_coordinate_or_raw_geometry_property(column_name: str, lat_col: str, lon_col: str, raw_geometry_cols: set[str]) -> bool:
    lowered = column_name.lower()
    if column_name in {lat_col, lon_col} or column_name in raw_geometry_cols:
        return True
    if lowered in {"latitude", "longitude", "lat", "lon", "lng"}:
        return True
    if lowered.endswith("_latitude") or lowered.endswith("_longitude"):
        return True
    if lowered in {"geometry_latitude", "geometry_longitude"}:
        return True
    return False


def export_points_geojson(
    source_name: str,
    output_path: str | None = None,
    limit: int | None = None,
    overwrite: bool = False,
    include_geojson: bool = False,
) -> dict[str, Any]:
    if source_name not in SUPPORTED_POINT_EXPORT_SOURCES:
        return {
            "ok": False,
            "source_name": source_name,
            "supported_sources": sorted(SUPPORTED_POINT_EXPORT_SOURCES),
            "error": "Source is not supported for initial point export.",
        }
    source = find_spatial_source(source_name)
    lat_col = source.get("latitude_column")
    lon_col = source.get("longitude_column")
    if not lat_col or not lon_col:
        raise ValueError(f"Source does not have latitude/longitude columns: {source_name}")
    alias, table = parse_source_name(source_name)
    safe_limit = max(1, int(limit)) if limit else None
    features: list[dict[str, Any]] = []
    with connect() as con:
        table_exists(con, alias, table)
        columns = [col["name"] for col in table_columns(con, alias, table)]
        raw_geometry_cols = set(source.get("geometry_columns_all") or [])
        raw_geometry_cols.update({"geometry_json", "properties_json", "geometry_wkt", "LOCATION_GEOMETRY_WKT", "respondent_geometry_wkt"})
        qtable = f"{quote_identifier(alias)}.{quote_identifier(table)}"
        where = (
            f"{quote_identifier(lat_col)} IS NOT NULL AND TRIM(CAST({quote_identifier(lat_col)} AS TEXT))<>'' "
            f"AND {quote_identifier(lon_col)} IS NOT NULL AND TRIM(CAST({quote_identifier(lon_col)} AS TEXT))<>''"
        )
        sql = f"SELECT * FROM {qtable} WHERE {where}"
        if safe_limit:
            sql += f" LIMIT {safe_limit}"
        for row in con.execute(sql):
            row_dict = dict(row)
            lat = to_float(row_dict.get(lat_col))
            lon = to_float(row_dict.get(lon_col))
            if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            properties = {
                key: compact_sample_value(value)
                for key, value in row_dict.items()
                if not is_coordinate_or_raw_geometry_property(key, lat_col, lon_col, raw_geometry_cols)
            }
            if source_name == "hvi.hvi_geojson_features":
                properties.update(parse_json_object(row_dict.get("properties_json")))
            features.append(geojson_feature(lon, lat, properties))
    collection = feature_collection(features)
    write_result = write_or_return_geojson(collection, output_path, source_default_filename(source_name), overwrite, include_geojson)
    return {
        "ok": True,
        "source_name": source_name,
        "feature_count": len(features),
        "crs": "EPSG:4326",
        "output_path": write_result["output_path"],
        "geojson": write_result["geojson"],
    }


def safe_generated_geojson_path(output_name: str) -> Path:
    if not output_name or not str(output_name).strip():
        raise ValueError("output_name must not be empty.")
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(output_name).strip())
    safe_name = safe_name.strip("._") or "generated_layer"
    if not safe_name.lower().endswith(".geojson"):
        safe_name += ".geojson"
    GENERATED_QGIS_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    candidate = GENERATED_QGIS_EXPORT_DIR / safe_name
    if not str(candidate.resolve()).startswith(str(GENERATED_QGIS_EXPORT_DIR.resolve())):
        raise ValueError("Resolved output path escapes generated export directory.")
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(2, 1000):
        next_candidate = GENERATED_QGIS_EXPORT_DIR / f"{stem}_{index}{suffix}"
        if not next_candidate.exists():
            return next_candidate
    raise FileExistsError(f"Could not allocate a generated output path for {safe_name}")


def export_sql_points_geojson(
    sql: str,
    latitude_column: str,
    longitude_column: str,
    output_name: str,
    label_column: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    validation = validate_sql_text(sql)
    if not validation.get("valid"):
        return {
            "ok": False,
            "validation": validation,
            "error": "SQL failed read-only validation.",
        }
    safe_limit = max(1, min(int(limit), MAX_QUERY_LIMIT))
    query_result = readonly_query(sql, safe_limit)
    columns = query_result.get("columns") or []
    if latitude_column not in columns or longitude_column not in columns:
        return {
            "ok": False,
            "validation": validation,
            "columns": columns,
            "error": "SQL result must include the requested latitude and longitude columns.",
            "latitude_column": latitude_column,
            "longitude_column": longitude_column,
        }
    if label_column and label_column not in columns:
        return {
            "ok": False,
            "validation": validation,
            "columns": columns,
            "error": "label_column was provided but is not present in the SQL result.",
            "label_column": label_column,
        }

    features: list[dict[str, Any]] = []
    skipped_missing_coordinates = 0
    skipped_invalid_coordinates = 0
    property_fields = [col for col in columns if col not in {latitude_column, longitude_column}]
    for row in query_result.get("rows") or []:
        lat_raw = row.get(latitude_column)
        lon_raw = row.get(longitude_column)
        if lat_raw is None or lon_raw is None or str(lat_raw).strip() == "" or str(lon_raw).strip() == "":
            skipped_missing_coordinates += 1
            continue
        lat = to_float(lat_raw)
        lon = to_float(lon_raw)
        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            skipped_invalid_coordinates += 1
            continue
        properties = {field: compact_sample_value(row.get(field)) for field in property_fields}
        if label_column:
            properties["_label"] = compact_sample_value(row.get(label_column))
        features.append(geojson_feature(lon, lat, properties))

    collection = feature_collection(features)
    output_path = safe_generated_geojson_path(output_name)
    output_path.write_text(json.dumps(collection, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    warnings: list[str] = []
    skipped_total = skipped_missing_coordinates + skipped_invalid_coordinates
    if skipped_total:
        warnings.append(
            f"Skipped {skipped_total} rows without usable coordinates "
            f"({skipped_missing_coordinates} missing, {skipped_invalid_coordinates} invalid)."
        )
    if query_result.get("warning"):
        warnings.append(str(query_result["warning"]))
    return {
        "ok": True,
        "validation": validation,
        "executed_sql": query_result.get("executed_sql"),
        "output_path": str(output_path),
        "feature_count": len(features),
        "latitude_column": latitude_column,
        "longitude_column": longitude_column,
        "property_fields": property_fields,
        "sample_features": features[:3],
        "rows_returned": query_result.get("row_count_returned"),
        "rows_skipped": skipped_total,
        "warning": " ".join(warnings) if warnings else None,
    }


def export_sensor_matched_points_geojson(
    group_by: str = "study_person_id",
    output_path: str | None = None,
    overwrite: bool = False,
    include_geojson: bool = False,
) -> dict[str, Any]:
    allowed_group_by = {"study_person_id", "resolved_respondent_id", "sensor_unique_id"}
    if group_by not in allowed_group_by:
        raise ValueError(f"group_by must be one of {sorted(allowed_group_by)}")
    qgroup = quote_identifier(group_by)
    with connect() as con:
        sql = f"""
            SELECT
              {qgroup} AS group_key,
              MIN(study_person_id) AS study_person_id,
              MIN(study_group) AS study_group,
              MIN(study_person_name) AS study_person_name,
              MIN(resolved_respondent_id) AS resolved_respondent_id,
              MIN(resolved_respondent_name) AS resolved_respondent_name,
              MIN(sensor_unique_id) AS sensor_unique_id,
              MIN(sensor_area) AS sensor_area,
              MIN(sensor_type) AS sensor_type,
              MIN(sensor_mapping_level) AS sensor_mapping_level,
              MIN(sensor_resolved_confidence) AS sensor_resolved_confidence,
              AVG(CAST(respondent_latitude AS REAL)) AS latitude,
              AVG(CAST(respondent_longitude AS REAL)) AS longitude,
              COUNT(*) AS sensor_row_count,
              ROUND(AVG(CAST(sensor_temperature_c AS REAL)), 4) AS avg_sensor_temperature_c,
              ROUND(MAX(CAST(sensor_temperature_c AS REAL)), 4) AS max_sensor_temperature_c,
              ROUND(AVG(CAST(sensor_humidity_rh AS REAL)), 4) AS avg_sensor_humidity_rh,
              ROUND(AVG(CAST(aws_heat_index_c AS REAL)), 4) AS avg_aws_heat_index_c,
              ROUND(MAX(CAST(aws_heat_index_c AS REAL)), 4) AS max_aws_heat_index_c,
              MIN(sensor_timestamp) AS first_timestamp,
              MAX(sensor_timestamp) AS last_timestamp
            FROM survey.SENSOR_AWS_RESPONDENT_ROWS
            WHERE {qgroup} IS NOT NULL
              AND TRIM(CAST({qgroup} AS TEXT)) <> ''
              AND respondent_latitude IS NOT NULL
              AND TRIM(CAST(respondent_latitude AS TEXT)) <> ''
              AND respondent_longitude IS NOT NULL
              AND TRIM(CAST(respondent_longitude AS TEXT)) <> ''
            GROUP BY {qgroup}
            ORDER BY sensor_row_count DESC, group_key
        """
        features = []
        for row in con.execute(sql):
            row_dict = dict(row)
            lat = to_float(row_dict.pop("latitude"))
            lon = to_float(row_dict.pop("longitude"))
            if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            features.append(geojson_feature(lon, lat, {k: compact_sample_value(v) for k, v in row_dict.items()}))
    collection = feature_collection(features)
    write_result = write_or_return_geojson(collection, output_path, "sensor_matched_points.geojson", overwrite, include_geojson)
    return {
        "ok": True,
        "source_name": "survey.SENSOR_AWS_RESPONDENT_ROWS",
        "group_by": group_by,
        "feature_count": len(features),
        "crs": "EPSG:4326",
        "output_path": write_result["output_path"],
        "geojson": write_result["geojson"],
    }


def get_spatial_join_map() -> dict[str, Any]:
    payload = load_spatial_join_map()
    return {"ok": True, **payload}


def qgis_check_environment() -> dict[str, Any]:
    errors: list[str] = []
    qgis_version = None
    processing_available = False
    qgis_prefix_path = None
    qgis_app_initialized = False
    headless_possible = False

    try:
        qgis_core = import_qgis_core()
        Qgis = qgis_core["Qgis"]
        QgsApplication = qgis_core["QgsApplication"]
        qgis_version = getattr(Qgis, "QGIS_VERSION", None)
        qgis_prefix_path = QgsApplication.prefixPath() or QgsApplication.pkgDataPath()
        try:
            ensure_qgis_application()
            qgis_app_initialized = True
            headless_possible = True
        except Exception as exc:
            errors.append(f"QGIS application initialization failed: {type(exc).__name__}: {exc}")
    except Exception as exc:
        errors.append(f"PyQGIS import failed: {type(exc).__name__}: {exc}")

    try:
        configure_qgis_plugin_path()
        __import__("processing")
        processing_available = True
    except Exception as exc:
        errors.append(f"processing import failed: {type(exc).__name__}: {exc}")

    pyqgis_available = qgis_version is not None
    ok = pyqgis_available and processing_available and qgis_app_initialized
    if ok:
        recommended_next_step = "PyQGIS appears available; qgis_create_project and QGIS map tools can be attempted."
    else:
        recommended_next_step = (
            "PyQGIS is unavailable or incomplete in this environment. Do not generate maps or run polygon joins here; "
            "use qgis_automation_plan documentation and QGIS-ready GeoJSON exports for manual QGIS work."
        )
    return {
        "ok": ok,
        "pyqgis_available": pyqgis_available,
        "qgis_version": qgis_version,
        "processing_available": processing_available,
        "qgis_prefix_path": qgis_prefix_path,
        "qgis_app_initialized": qgis_app_initialized,
        "headless_possible": headless_possible,
        "errors": errors,
        "recommended_next_step": recommended_next_step,
    }


def configure_qgis_plugin_path() -> str | None:
    try:
        import qgis  # type: ignore
    except Exception:
        return None
    qgis_file = getattr(qgis, "__file__", None)
    if not qgis_file:
        return None
    plugin_dir = Path(qgis_file).resolve().parents[1] / "plugins"
    if plugin_dir.exists() and str(plugin_dir) not in sys.path:
        sys.path.append(str(plugin_dir))
    return str(plugin_dir) if plugin_dir.exists() else None


def import_qgis_core() -> dict[str, Any]:
    configure_qgis_plugin_path()
    from qgis.core import (  # type: ignore
        Qgis,
        QgsApplication,
        QgsCategorizedSymbolRenderer,
        QgsCoordinateReferenceSystem,
        QgsFeature,
        QgsField,
        QgsGraduatedSymbolRenderer,
        QgsLayout,
        QgsLayoutExporter,
        QgsLayoutItemLabel,
        QgsLayoutItemMap,
        QgsLayoutPoint,
        QgsLayoutSize,
        QgsMapRendererParallelJob,
        QgsMapSettings,
        QgsProject,
        QgsRendererCategory,
        QgsStyle,
        QgsSymbol,
        QgsUnitTypes,
        QgsVectorFileWriter,
        QgsVectorLayer,
        QgsWkbTypes,
    )
    return locals()


def ensure_qgis_application() -> Any:
    global _QGIS_APP
    qgis_core = import_qgis_core()
    QgsApplication = qgis_core["QgsApplication"]
    if _QGIS_APP is None:
        _QGIS_APP = QgsApplication([], False)
        _QGIS_APP.initQgis()
    return _QGIS_APP


def resolve_project_path(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    return candidate.resolve()


def qgis_layer_by_name(layer_name: str) -> Any:
    qgis_core = import_qgis_core()
    QgsProject = qgis_core["QgsProject"]
    matches = QgsProject.instance().mapLayersByName(layer_name)
    if not matches:
        raise ValueError(f"No QGIS layer loaded with name: {layer_name}")
    return matches[0]


def qgis_layer_metadata(layer: Any) -> dict[str, Any]:
    qgis_core = import_qgis_core()
    QgsWkbTypes = qgis_core["QgsWkbTypes"]
    return {
        "ok": True,
        "layer_name": layer.name(),
        "feature_count": int(layer.featureCount()),
        "geometry_type": QgsWkbTypes.displayString(layer.wkbType()),
        "crs": layer.crs().authid() or layer.crs().description(),
        "fields": [field.name() for field in layer.fields()],
        "errors": [],
    }


def qgis_unavailable_response(tool_name: str) -> dict[str, Any]:
    env = qgis_check_environment()
    return {
        "ok": False,
        "tool_name": tool_name,
        "active": False,
        "stub": True,
        "errors": env["errors"] or ["PyQGIS environment is not available."],
        "environment": env,
        "recommended_next_step": "Install/configure QGIS/PyQGIS and rerun qgis_check_environment before using this tool.",
    }


def qgis_load_geojson_layer(path: str, layer_name: str | None = None) -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_load_geojson_layer"), "layer_name": layer_name, "path": path}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsProject = qgis_core["QgsProject"]
        QgsVectorLayer = qgis_core["QgsVectorLayer"]
        layer_path = resolve_project_path(path)
        if not layer_path.exists():
            return {"ok": False, "layer_name": layer_name, "path": str(layer_path), "errors": [f"File does not exist: {path}"]}
        if layer_path.suffix.lower() != ".geojson":
            return {"ok": False, "layer_name": layer_name, "path": str(layer_path), "errors": ["Only GeoJSON files are supported initially."]}
        allowed = {value.resolve() for value in STANDARD_QGIS_GEOJSONS.values()}
        if layer_path not in allowed:
            return {
                "ok": False,
                "layer_name": layer_name,
                "path": str(layer_path),
                "supported_initial_files": [str(value) for value in STANDARD_QGIS_GEOJSONS.values()],
                "errors": ["Path is not one of the supported initial QGIS export files."],
            }
        layer = QgsVectorLayer(str(layer_path), layer_name or layer_path.stem, "ogr")
        if not layer.isValid():
            return {"ok": False, "layer_name": layer_name or layer_path.stem, "path": str(layer_path), "errors": ["QGIS could not load layer."]}
        QgsProject.instance().addMapLayer(layer)
        return {**qgis_layer_metadata(layer), "path": str(layer_path)}
    except Exception as exc:
        return {"ok": False, "layer_name": layer_name, "path": path, "errors": [f"{type(exc).__name__}: {exc}"]}


def qgis_create_project(project_path: str, layer_paths: list[str] | None = None) -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_create_project"), "project_path": project_path, "loaded_layers": [], "requested_layer_paths": layer_paths or []}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsProject = qgis_core["QgsProject"]
        project = QgsProject.instance()
        project.clear()
        paths = layer_paths if layer_paths is not None else [str(path) for path in STANDARD_QGIS_GEOJSONS.values()]
        loaded_layers = []
        for layer_path in paths:
            loaded = qgis_load_geojson_layer(layer_path)
            if loaded.get("ok"):
                loaded_layers.append(loaded)
        out_path = resolve_project_path(project_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        ok = bool(project.write(str(out_path)))
        return {"ok": ok, "project_path": str(out_path), "loaded_layers": loaded_layers, "errors": [] if ok else ["QGIS project write failed."]}
    except Exception as exc:
        return {"ok": False, "project_path": project_path, "loaded_layers": [], "errors": [f"{type(exc).__name__}: {exc}"]}


def qgis_apply_categorized_style(layer_name: str, field_name: str, categories: dict[str, Any] | None = None) -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_apply_categorized_style"), "layer_name": layer_name, "field_name": field_name, "category_count": len(categories or {}), "applied_style_summary": None}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsCategorizedSymbolRenderer = qgis_core["QgsCategorizedSymbolRenderer"]
        QgsProject = qgis_core["QgsProject"]
        QgsRendererCategory = qgis_core["QgsRendererCategory"]
        QgsSymbol = qgis_core["QgsSymbol"]
        layer = qgis_layer_by_name(layer_name)
        idx = layer.fields().indexFromName(field_name)
        if idx < 0:
            return {"ok": False, "layer_name": layer_name, "field_name": field_name, "category_count": 0, "applied_style_summary": None, "errors": [f"Field not found: {field_name}"]}
        values = sorted({str(value) for value in layer.uniqueValues(idx)})
        renderer_categories = []
        for value in values:
            symbol = QgsSymbol.defaultSymbol(layer.geometryType())
            renderer_categories.append(QgsRendererCategory(value, symbol, value))
        layer.setRenderer(QgsCategorizedSymbolRenderer(field_name, renderer_categories))
        layer.triggerRepaint()
        if QgsProject.instance().fileName():
            QgsProject.instance().write()
        return {
            "ok": True,
            "layer_name": layer_name,
            "field_name": field_name,
            "category_count": len(values),
            "applied_style_summary": {"type": "categorized", "categories": values},
            "errors": [],
        }
    except Exception as exc:
        return {"ok": False, "layer_name": layer_name, "field_name": field_name, "category_count": 0, "applied_style_summary": None, "errors": [f"{type(exc).__name__}: {exc}"]}


def qgis_apply_graduated_style(layer_name: str, field_name: str, classes: int = 5, mode: str = "quantile") -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_apply_graduated_style"), "layer_name": layer_name, "field_name": field_name, "classes": classes, "mode": mode, "min_value": None, "max_value": None, "applied_style_summary": None}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsGraduatedSymbolRenderer = qgis_core["QgsGraduatedSymbolRenderer"]
        QgsProject = qgis_core["QgsProject"]
        QgsStyle = qgis_core["QgsStyle"]
        QgsSymbol = qgis_core["QgsSymbol"]
        layer = qgis_layer_by_name(layer_name)
        idx = layer.fields().indexFromName(field_name)
        if idx < 0:
            return {"ok": False, "layer_name": layer_name, "field_name": field_name, "classes": classes, "mode": mode, "min_value": None, "max_value": None, "applied_style_summary": None, "errors": [f"Field not found: {field_name}"]}
        values = []
        for feature in layer.getFeatures():
            val = to_float(feature[field_name])
            if val is not None:
                values.append(val)
        if not values:
            return {"ok": False, "layer_name": layer_name, "field_name": field_name, "classes": classes, "mode": mode, "min_value": None, "max_value": None, "applied_style_summary": None, "errors": ["No numeric values found for graduated style."]}
        mode_lookup = {
            "quantile": QgsGraduatedSymbolRenderer.Quantile,
            "equal_interval": QgsGraduatedSymbolRenderer.EqualInterval,
            "jenks": QgsGraduatedSymbolRenderer.Jenks,
            "pretty": QgsGraduatedSymbolRenderer.Pretty,
        }
        symbol = QgsSymbol.defaultSymbol(layer.geometryType())
        ramp = QgsStyle().defaultStyle().colorRamp("Viridis")
        renderer = QgsGraduatedSymbolRenderer.createRenderer(layer, field_name, int(classes), mode_lookup.get(mode, QgsGraduatedSymbolRenderer.Quantile), symbol, ramp)
        layer.setRenderer(renderer)
        layer.triggerRepaint()
        if QgsProject.instance().fileName():
            QgsProject.instance().write()
        return {
            "ok": True,
            "layer_name": layer_name,
            "field_name": field_name,
            "classes": int(classes),
            "mode": mode,
            "min_value": min(values),
            "max_value": max(values),
            "applied_style_summary": {"type": "graduated", "range_count": len(renderer.ranges()) if renderer else None},
            "errors": [],
        }
    except Exception as exc:
        return {"ok": False, "layer_name": layer_name, "field_name": field_name, "classes": classes, "mode": mode, "min_value": None, "max_value": None, "applied_style_summary": None, "errors": [f"{type(exc).__name__}: {exc}"]}


def qgis_export_map_png(project_path: str, output_path: str, title: str | None = None, width: int = 1600, height: int = 1200) -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_export_map_png"), "project_path": project_path, "output_path": output_path, "title": title, "width": width, "height": height, "layers_rendered": 0}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsProject = qgis_core["QgsProject"]
        QgsMapSettings = qgis_core["QgsMapSettings"]
        QgsMapRendererParallelJob = qgis_core["QgsMapRendererParallelJob"]
        from qgis.PyQt.QtCore import QSize  # type: ignore
        from qgis.PyQt.QtGui import QColor, QImage, QPainter  # type: ignore

        project = QgsProject.instance()
        if project_path and Path(project_path).exists() and not project.mapLayers():
            project.read(str(resolve_project_path(project_path)))
        layers = list(project.mapLayers().values())
        if not layers:
            return {"ok": False, "output_path": output_path, "width": width, "height": height, "layers_rendered": 0, "errors": ["No layers loaded in QGIS project."]}
        extent = layers[0].extent()
        for layer in layers[1:]:
            extent.combineExtentWith(layer.extent())
        image = QImage(QSize(int(width), int(height)), QImage.Format_ARGB32_Premultiplied)
        image.fill(QColor("white"))
        painter = QPainter(image)
        settings = QgsMapSettings()
        settings.setLayers(layers)
        settings.setExtent(extent)
        settings.setOutputSize(image.size())
        job = QgsMapRendererParallelJob(settings)
        job.start()
        job.waitForFinished()
        rendered = job.renderedImage()
        painter.drawImage(0, 0, rendered)
        painter.end()
        out_path = resolve_project_path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        ok = bool(image.save(str(out_path), "PNG"))
        return {"ok": ok, "output_path": str(out_path), "width": int(width), "height": int(height), "layers_rendered": len(layers), "errors": [] if ok else ["Image save failed."]}
    except Exception as exc:
        return {"ok": False, "output_path": output_path, "width": width, "height": height, "layers_rendered": 0, "errors": [f"{type(exc).__name__}: {exc}"]}


def qgis_export_map_pdf(project_path: str, output_path: str, title: str | None = None) -> dict[str, Any]:
    env = qgis_check_environment()
    if not env["ok"]:
        return {**qgis_unavailable_response("qgis_export_map_pdf"), "project_path": project_path, "output_path": output_path, "title": title, "layers_rendered": 0}
    try:
        ensure_qgis_application()
        qgis_core = import_qgis_core()
        QgsProject = qgis_core["QgsProject"]
        QgsLayout = qgis_core["QgsLayout"]
        QgsLayoutExporter = qgis_core["QgsLayoutExporter"]
        QgsLayoutItemMap = qgis_core["QgsLayoutItemMap"]
        QgsLayoutItemLabel = qgis_core["QgsLayoutItemLabel"]
        QgsUnitTypes = qgis_core["QgsUnitTypes"]
        from qgis.PyQt.QtGui import QFont  # type: ignore

        project = QgsProject.instance()
        if project_path and Path(project_path).exists() and not project.mapLayers():
            project.read(str(resolve_project_path(project_path)))
        layers = list(project.mapLayers().values())
        if not layers:
            return {"ok": False, "output_path": output_path, "layers_rendered": 0, "errors": ["No layers loaded in QGIS project."]}
        extent = layers[0].extent()
        for layer in layers[1:]:
            extent.combineExtentWith(layer.extent())
        layout = QgsLayout(project)
        layout.initializeDefaults()
        map_item = QgsLayoutItemMap(layout)
        map_item.setRect(20, 20, 240, 160)
        map_item.setExtent(extent)
        layout.addLayoutItem(map_item)
        map_item.attemptMove(qgis_core["QgsLayoutPoint"](10, 25, QgsUnitTypes.LayoutMillimeters))
        map_item.attemptResize(qgis_core["QgsLayoutSize"](277, 170, QgsUnitTypes.LayoutMillimeters))
        if title:
            label = QgsLayoutItemLabel(layout)
            label.setText(title)
            label.setFont(QFont("Arial", 16))
            layout.addLayoutItem(label)
        out_path = resolve_project_path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result = QgsLayoutExporter(layout).exportToPdf(str(out_path), QgsLayoutExporter.PdfExportSettings())
        ok = result == QgsLayoutExporter.Success
        return {"ok": ok, "output_path": str(out_path), "layers_rendered": len(layers), "errors": [] if ok else [f"PDF export failed with code {result}."]}
    except Exception as exc:
        return {"ok": False, "output_path": output_path, "layers_rendered": 0, "errors": [f"{type(exc).__name__}: {exc}"]}


def geojson_file_summary(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "geometry_type": None,
            "crs": None,
            "feature_count": None,
            "usable_for_polygon_join": False,
            "errors": [f"Could not read GeoJSON: {type(exc).__name__}: {exc}"],
        }
    geometry_types: Counter[str] = Counter()
    features = payload.get("features") if isinstance(payload, dict) else None
    if isinstance(features, list):
        for feature in features:
            geom = (feature or {}).get("geometry") or {}
            if geom.get("type"):
                geometry_types[str(geom["type"])] += 1
    geometry_type = ",".join(sorted(geometry_types)) if geometry_types else payload.get("type")
    crs = None
    if isinstance(payload, dict) and isinstance(payload.get("crs"), dict):
        crs = payload["crs"].get("properties", {}).get("name")
    usable = bool(geometry_types) and set(geometry_types).issubset({"Polygon", "MultiPolygon"})
    return {
        "geometry_type": geometry_type,
        "crs": crs or "unknown",
        "feature_count": len(features) if isinstance(features, list) else None,
        "usable_for_polygon_join": usable,
        "errors": [],
    }


def sqlite_spatial_summary(path: Path) -> dict[str, Any]:
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        con = sqlite3.connect(uri, uri=True)
        try:
            tables = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            spatial_tables = [name for name in tables if name.lower() in {"geometry_columns", "gpkg_geometry_columns"}]
            return {
                "geometry_type": "unknown",
                "crs": "unknown",
                "feature_count": None,
                "usable_for_polygon_join": False,
                "errors": [] if spatial_tables else ["No spatial metadata table detected."],
            }
        finally:
            con.close()
    except Exception as exc:
        return {
            "geometry_type": None,
            "crs": None,
            "feature_count": None,
            "usable_for_polygon_join": False,
            "errors": [f"Could not inspect SQLite spatial metadata: {type(exc).__name__}: {exc}"],
        }


def list_polygon_boundary_sources() -> dict[str, Any]:
    candidates = []
    suffixes = {".shp", ".geojson", ".gpkg", ".sqlite"}
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in suffixes:
            continue
        relative = str(path.relative_to(ROOT))
        if path.suffix.lower() == ".geojson":
            summary = geojson_file_summary(path)
        elif path.suffix.lower() == ".sqlite":
            summary = sqlite_spatial_summary(path)
        else:
            summary = {
                "geometry_type": "unknown without PyQGIS/GDAL",
                "crs": "unknown without PyQGIS/GDAL",
                "feature_count": None,
                "usable_for_polygon_join": False,
                "errors": ["PyQGIS/GDAL unavailable; cannot verify geometry type."],
            }
        candidates.append(
            {
                "path": relative,
                "file_name": path.name,
                "geometry_type": summary["geometry_type"],
                "crs": summary["crs"],
                "feature_count": summary["feature_count"],
                "usable_for_polygon_join": summary["usable_for_polygon_join"],
                "errors": summary["errors"],
            }
        )
    return {
        "ok": True,
        "candidate_files": candidates,
        "usable_polygon_boundary_count": sum(1 for row in candidates if row["usable_for_polygon_join"]),
        "notes": [
            "Do not claim polygon aggregation unless usable_for_polygon_join is true for a boundary source.",
            "Point GeoJSON exports are not polygon boundaries.",
        ],
    }


def qgis_polygon_join_points_to_boundary(
    point_layer_path: str,
    polygon_layer_path: str,
    output_path: str,
    summary_fields: list[str] | None = None,
) -> dict[str, Any]:
    point_path = (ROOT / point_layer_path).resolve() if not Path(point_layer_path).is_absolute() else Path(point_layer_path)
    polygon_path = (ROOT / polygon_layer_path).resolve() if not Path(polygon_layer_path).is_absolute() else Path(polygon_layer_path)
    errors = []
    if not point_path.exists():
        errors.append(f"Point layer does not exist: {point_layer_path}")
    if not polygon_path.exists():
        errors.append(f"Polygon layer does not exist: {polygon_layer_path}")
    if polygon_path.exists() and polygon_path.suffix.lower() == ".geojson":
        summary = geojson_file_summary(polygon_path)
        if not summary["usable_for_polygon_join"]:
            errors.append(f"Boundary layer is not polygon/multipolygon: {summary['geometry_type']}")
    if errors:
        return {
            "ok": False,
            "output_path": output_path,
            "polygon_count": None,
            "joined_point_count": None,
            "summary_fields": summary_fields or [],
            "errors": errors,
        }
    return {
        **qgis_unavailable_response("qgis_polygon_join_points_to_boundary"),
        "output_path": output_path,
        "polygon_count": None,
        "joined_point_count": None,
        "summary_fields": summary_fields or [],
    }


def load_rag_chunks() -> list[dict[str, Any]]:
    if not RAG_CHUNKS_PATH.exists():
        raise FileNotFoundError(f"Missing RAG chunks file: {RAG_CHUNKS_PATH.name}")
    chunks = []
    with RAG_CHUNKS_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                chunks.append(json.loads(line))
    return chunks


def load_json_file(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path.name}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path.name}")
    return path.read_text(encoding="utf-8")


def normalize_filter_value(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        values = value
    else:
        values = [value]
    normalized = []
    for item in values:
        if item is None:
            continue
        for part in str(item).split(";"):
            text = part.strip()
            if text:
                normalized.append(text)
    return normalized or None


def normalize_rag_filters(filters: dict[str, Any] | None = None) -> dict[str, list[str] | None]:
    filters = filters or {}
    return {
        "file_name": normalize_filter_value(filters.get("file_name")),
        "document_category": normalize_filter_value(filters.get("document_category")),
        "topic_tags": normalize_filter_value(filters.get("topic_tags")),
        "content_type": normalize_filter_value(filters.get("content_type")),
    }


def list_value_matches(values: Any, filters: list[str] | None) -> bool:
    if not filters:
        return True
    if values is None:
        return False
    if isinstance(values, list):
        haystack = {str(value).lower() for value in values}
    else:
        haystack = {str(values).lower()}
    return bool(haystack & {value.lower() for value in filters})


def rag_chunk_matches_filters(chunk: dict[str, Any], filters: dict[str, list[str] | None]) -> bool:
    file_names = filters.get("file_name")
    if file_names and chunk.get("file_name", "").lower() not in {name.lower() for name in file_names}:
        return False
    if not list_value_matches(chunk.get("document_category"), filters.get("document_category")):
        return False
    if not list_value_matches(chunk.get("topic_tags"), filters.get("topic_tags")):
        return False
    content_types = filters.get("content_type")
    if content_types and chunk.get("content_type", "").lower() not in {value.lower() for value in content_types}:
        return False
    return True


def rag_metadata(chunk: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "doc_id",
        "file_name",
        "source_path",
        "document_category",
        "topic_tags",
        "content_type",
        "chunk_index",
        "word_count",
        "paragraph_start",
        "paragraph_end",
        "table_count",
        "image_count",
    ]
    return {key: chunk.get(key) for key in keys}


def rag_preview(text: str, max_chars: int = 280) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def import_rag_search_module() -> Any:
    import importlib.util

    module_path = ROOT / "rag_search.py"
    if not module_path.exists():
        raise FileNotFoundError("Missing rag_search.py")
    spec = importlib.util.spec_from_file_location("local_rag_search", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load rag_search.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def list_rag_documents() -> dict[str, Any]:
    chunks = load_rag_chunks()
    by_doc: dict[str, dict[str, Any]] = {}
    for chunk in chunks:
        doc_id = chunk.get("doc_id")
        if not doc_id:
            continue
        entry = by_doc.setdefault(
            doc_id,
            {
                "doc_id": doc_id,
                "file_name": chunk.get("file_name"),
                "document_category": chunk.get("document_category", []),
                "topic_tags": chunk.get("topic_tags", []),
                "chunk_count": 0,
                "word_count": 0,
            },
        )
        entry["chunk_count"] += 1
        entry["word_count"] += int(chunk.get("word_count") or 0)
    return {"ok": True, "documents": sorted(by_doc.values(), key=lambda row: row["file_name"] or ""), "document_count": len(by_doc)}


def search_rag_chunks(query: str, top_k: int = 5, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    if not query.strip():
        raise ValueError("query is required")
    safe_top_k = max(1, min(int(top_k), 20))
    normalized_filters = normalize_rag_filters(filters)
    rag_search = import_rag_search_module()
    chunks = rag_search.load_chunks(RAG_CHUNKS_PATH)
    index = rag_search.BM25Index(chunks)
    raw_results = index.search(query, top_k=safe_top_k, filters=normalized_filters)
    results = []
    for rank, row in enumerate(raw_results, start=1):
        results.append(
            {
                "rank": rank,
                "chunk_id": row.get("chunk_id"),
                "score": row.get("score"),
                "file_name": row.get("file_name"),
                "text_preview": row.get("text_preview"),
                "metadata": row.get("metadata"),
            }
        )
    return {
        "ok": True,
        "query": query,
        "top_k": safe_top_k,
        "filters": normalized_filters,
        "retrieval_method": "local BM25 keyword search over rag_chunks.jsonl",
        "results": results,
    }


def get_rag_chunk(chunk_id: str) -> dict[str, Any]:
    chunk = next((item for item in load_rag_chunks() if item.get("chunk_id") == chunk_id), None)
    if not chunk:
        raise ValueError(f"Unknown chunk_id: {chunk_id}")
    return {
        "ok": True,
        "chunk_id": chunk_id,
        "text": chunk.get("text", ""),
        "metadata": rag_metadata(chunk),
    }


def filter_rag_chunks(
    file_name: Any = None,
    document_category: Any = None,
    topic_tags: Any = None,
    content_type: Any = None,
    limit: int = 20,
) -> dict[str, Any]:
    filters = normalize_rag_filters(
        {
            "file_name": file_name,
            "document_category": document_category,
            "topic_tags": topic_tags,
            "content_type": content_type,
        }
    )
    safe_limit = max(1, min(int(limit), 100))
    matches = []
    for chunk in load_rag_chunks():
        if rag_chunk_matches_filters(chunk, filters):
            matches.append(
                {
                    "chunk_id": chunk.get("chunk_id"),
                    "file_name": chunk.get("file_name"),
                    "text_preview": rag_preview(chunk.get("text", "")),
                    "metadata": rag_metadata(chunk),
                }
            )
        if len(matches) >= safe_limit:
            break
    return {"ok": True, "filters": filters, "limit": safe_limit, "chunks": matches, "chunk_count_returned": len(matches)}


def rag_corpus_summary() -> dict[str, Any]:
    chunks = load_rag_chunks()
    docs = list_rag_documents()["documents"]
    content_types = Counter(chunk.get("content_type") for chunk in chunks)
    topic_tags: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    for chunk in chunks:
        for tag in chunk.get("topic_tags") or []:
            topic_tags[str(tag)] += 1
        for category in chunk.get("document_category") or []:
            categories[str(category)] += 1
    limitations = [
        "RAG tools return evidence chunks only; final answer synthesis is the MCP client/LLM's job.",
        "BM25 is lexical and deterministic; it does not use embeddings or semantic vector search.",
        "No external LLM APIs are called.",
        "Image content is not interpreted; image counts and explicit captions are metadata only.",
        "The writing corpus is qualitative context, not the source of truth for SQL-derived HVI scores or sensor values.",
    ]
    return {
        "ok": True,
        "total_documents": len(docs),
        "total_chunks": len(chunks),
        "content_type_distribution": dict(sorted(content_types.items())),
        "topic_tag_distribution": dict(sorted(topic_tags.items())),
        "document_category_distribution": dict(sorted(categories.items())),
        "limitations": limitations,
    }


def resource_payload(uri: str) -> str:
    if uri == "mcp://unsupported-question-policy":
        return json.dumps(
            {
                "purpose": "Policy for unsupported questions and safe redirection.",
                "server_scope": (
                    "The MCP server only supports questions grounded in connected SQL databases, "
                    "spatial/QGIS outputs, and RAG document chunks."
                ),
                "server_role": (
                    "The MCP server provides safe tools, resources, prompts, and guardrails. "
                    "The MCP client/LLM performs reasoning and final answer generation."
                ),
                "unsupported_questions": [
                    "Predicting individual death or identifying who will die.",
                    "Predicting medical outcomes, diagnoses, or individual health events.",
                    "Predicting future disasters.",
                    "Providing live weather forecasts or tomorrow's weather.",
                    "Answering claims not grounded in connected SQL, spatial/QGIS, or RAG sources.",
                ],
                "redirects": {
                    "who_will_die": (
                        "Do not predict death. Redirect to heat vulnerability/risk indicator analysis, "
                        "such as highest HVI scores/categories, high heat exposure in matched sensor data, "
                        "age/illness/housing/water-access risk indicators, or group-level vulnerability summaries."
                    ),
                    "tomorrows_weather": (
                        "Explain that no live weather API is connected. Offer historical sensor/AWS analysis "
                        "from the survey database, such as past temperature, humidity, heat-index summaries, "
                        "or matched sensor subset patterns."
                    ),
                },
                "evidence_boundaries": {
                    "sql": "Use SQL for exact database facts, counts, averages, rankings, grouped summaries, HVI/survey/sensor analysis.",
                    "spatial_qgis": "Use spatial/QGIS tools for coordinate quality, GeoJSON exports, QGIS-ready layers, and existing map outputs.",
                    "rag": "Use RAG for qualitative document context only, not exact counts, HVI scores, sensor values, or coordinates.",
                },
            },
            indent=2,
        )
    if uri == "mcp://safety-and-scope-policy":
        return json.dumps(
            {
                "purpose": "System-wide safety and scope guidance for MCP-compatible clients.",
                "core_scope": [
                    "Connected SQL databases: survey and HVI.",
                    "Spatial/QGIS readiness data, GeoJSON exports, and existing QGIS map outputs.",
                    "Local BM25 RAG search over generated document chunks.",
                ],
                "not_supported": [
                    "Individual death prediction.",
                    "Medical outcome prediction or diagnosis.",
                    "Future disaster prediction.",
                    "Live weather forecasts.",
                    "External facts not present in connected sources.",
                ],
                "safe_redirection_rules": [
                    "If asked 'who will die', state that the system cannot predict death and offer heat vulnerability/risk indicator analysis instead.",
                    "If asked about tomorrow's weather, state that no live weather API is connected and offer historical sensor/AWS analysis instead.",
                    "If a question requires exact counts or values, use SQL rather than RAG.",
                    "If a question requires maps, coordinates, or QGIS exports, use spatial/QGIS tools.",
                    "If a question asks for qualitative context, use RAG chunks and cite chunk IDs/file names.",
                ],
                "client_responsibility": (
                    "The MCP client/LLM chooses tools, reasons over returned evidence, and writes the final answer. "
                    "The server exposes safe, scoped tools and guardrail resources/prompts."
                ),
                "final_answer_requirement": (
                    "Final answers should identify whether evidence came from SQL results, spatial/QGIS outputs, "
                    "or RAG chunks, and should not invent unsupported predictions."
                ),
            },
            indent=2,
        )
    if uri == "qgis://environment":
        return json.dumps(qgis_check_environment(), indent=2)
    if uri == "qgis://standard-layer-style-guide":
        return json.dumps(
            {
                "purpose": "Style recommendations for QGIS-ready GeoJSON point layers.",
                "layers": {
                    "hvi_points": {
                        "path": "qgis_exports/hvi_points.geojson",
                        "categorical_fields": ["hvi_category", "source_db"],
                        "graduated_fields": ["hvi_answer_level_score"],
                    },
                    "urban_household_points": {
                        "path": "qgis_exports/urban_household_points.geojson",
                        "categorical_fields": ["place"],
                        "graduated_fields": [],
                    },
                    "main_respondent_points": {
                        "path": "qgis_exports/main_respondent_points.geojson",
                        "categorical_fields": ["SELECT_THE_ZONE_OF_STUDY"],
                        "graduated_fields": [],
                    },
                    "sensor_matched_points": {
                        "path": "qgis_exports/sensor_matched_points.geojson",
                        "categorical_fields": ["group_key"],
                        "graduated_fields": ["avg_sensor_temperature_c", "avg_aws_heat_index_c", "max_sensor_temperature_c"],
                    },
                },
                "rules": [
                    "Use categorized style for text/category fields.",
                    "Use graduated style for numeric score, temperature, humidity, or heat-index fields.",
                    "Always report CRS assumptions and layer limitations.",
                ],
            },
            indent=2,
        )
    if uri == "qgis://map-export-guide":
        return json.dumps(
            {
                "purpose": "Guide for optional PyQGIS map exports.",
                "preconditions": [
                    "Run qgis_check_environment first.",
                    "Only attempt automated exports if ok=true.",
                    "Use SQL tools for exact numeric summaries before mapping.",
                    "Use RAG only for qualitative interpretation context.",
                ],
                "standard_outputs_if_available": [
                    "qgis_automated_outputs/singareni_heat_vulnerability.qgz",
                    "qgis_automated_outputs/hvi_category_map.png",
                    "qgis_automated_outputs/hvi_score_map.png",
                    "qgis_automated_outputs/sensor_temperature_map.png",
                    "qgis_automated_outputs/qgis_automation_metadata.json",
                ],
            },
            indent=2,
        )
    if uri == "qgis://polygon-join-requirements":
        return json.dumps(
            {
                "requirements": [
                    "A valid polygon or multipolygon boundary layer must be available.",
                    "Point and polygon layer paths must exist.",
                    "CRS compatibility must be checked; reproject if needed in a PyQGIS-capable environment.",
                    "Do not claim polygon aggregation unless polygon boundary data was used.",
                    "Do not modify original files; write joined output to GeoJSON or GeoPackage.",
                ],
                "recommended_first_tool": "list_polygon_boundary_sources",
                "current_environment": qgis_check_environment(),
            },
            indent=2,
        )
    if uri == "mcp://analysis-routing-guide":
        return json.dumps(
            {
                "server_role": "The MCP server exposes tools, resources, and prompts; the MCP client/LLM performs reasoning and final answer generation.",
                "routing_rules": [
                    {
                        "use_sql_for": [
                            "exact database facts",
                            "counts, averages, sums, min/max, percentages",
                            "rankings and grouped summaries",
                            "HVI scores, HVI categories, indicators, validation, evidence tables",
                            "field/survey/urban/sensor/AWS analysis",
                        ],
                        "do_not_use_sql_for": ["qualitative narrative interpretation from Word documents", "GeoJSON export"],
                    },
                    {
                        "use_spatial_for": [
                            "coordinate quality",
                            "spatial source discovery",
                            "GeoJSON point export",
                            "QGIS-ready layer preparation",
                        ],
                        "do_not_use_spatial_for": ["exact database aggregates", "HVI score calculation", "qualitative document context"],
                    },
                    {
                        "use_rag_for": [
                            "qualitative narrative/document context from Word documents",
                            "themes such as water, housing, livelihood, gender, land tenure, and lived heat experience",
                        ],
                        "do_not_use_rag_for": ["exact counts", "HVI scores", "sensor values", "coordinates", "database-derived categories"],
                    },
                ],
                "hvi_rule": "Do not use HVI tables unless the question asks about HVI scores, categories, indicators, vulnerability, or HVI evidence.",
                "final_answer_rule": "Mention whether evidence came from SQL results, spatial tools, or RAG chunks.",
                "recommended_first_step": "Classify the question as SQL, spatial, RAG, or combined before calling tools.",
            },
            indent=2,
        )
    if uri == "sqlite://field-survey-table-family-guide":
        return json.dumps(
            {
                "server_role": "The MCP server provides schema and join guidance; the client/LLM chooses tables and writes SQL.",
                "table_families": {
                    "respondent_tables": {
                        "pattern": "RESPONDENT_* and RESPONDENT",
                        "join_keys": ["RESPONDENT_ID", "RESPONDENT_UUID"],
                        "use_for": "Main respondent-level survey facts and modules.",
                    },
                    "family_member_tables": {
                        "pattern": "FAMILY_MEMBER_* and family-member modules",
                        "join_keys": ["RESPONDENT_UUID", "SUBMISSION_UUID"],
                        "use_for": "Family-member-level summaries, counts, vulnerability indicators, illness/disability/education/occupation modules.",
                    },
                    "urban_tables": {
                        "pattern": "URBAN_* including URBAN_RESPONDENTS, URBAN_SHAPES, URBAN_COOK_TIME, URBAN_ILLNESS",
                        "join_keys": ["household_id"],
                        "use_for": "Urban household-level and repeat-module analysis.",
                    },
                    "sensor_tables": {
                        "pattern": "SENSOR_AWS_RESPONDENT_ROWS and SENSOR_AWS_*_SUMMARY",
                        "join_keys": ["study_person_id"],
                        "use_for": "Sensor/AWS raw-row or summary-level heat metrics.",
                    },
                    "people_170": {
                        "role": "Bridge table linking HVI, main respondents, urban households, and sensor study_person_id.",
                        "warning": "PEOPLE_170 is not always the analysis anchor; use it when bridging identities or joining sources.",
                    },
                    "hvi_tables": {
                        "join_to_people_170": "Use hvi_scores_to_people_170 source-dependent join template.",
                        "avoid_when": "Avoid HVI tables for non-HVI questions.",
                    },
                },
                "grain_warning": "Decide whether the answer should be respondent-level, family-member-level, household-level, sensor-row-level, or summary-level before SQL.",
            },
            indent=2,
        )
    if uri == "sqlite://result-shape-planning-guide":
        return json.dumps(
            {
                "server_role": "The server validates and executes SQL; the client/LLM must plan the result shape before writing SQL.",
                "required_before_sql": [
                    "expected_answer_type: exact_number, grouped_summary, ranked_list, comparison, row_level_listing, explanation",
                    "expected output columns and aliases",
                    "grouping level",
                    "metrics needed: COUNT, AVG, SUM, MIN, MAX, percentage",
                    "filters and thresholds",
                    "joins and known join templates",
                    "ORDER BY and LIMIT",
                    "analysis grain: respondent-level, family-member-level, household-level, sensor-row-level, or summary-level",
                ],
                "rules": [
                    "Do not return detail rows when grouped summary is requested.",
                    "Do not group by extra columns just because they are available.",
                    "Include every requested COUNT/AVG/SUM/MIN/MAX/percentage metric.",
                    "Use clear aliases that match the requested meaning.",
                    "For final answers, check that the result columns answer the exact wording.",
                ],
            },
            indent=2,
        )
    if uri == "sqlite://agent-failure-lessons":
        return json.dumps(
            {
                "observed_failure_modes": [
                    "Returning detail rows when grouped summary is needed.",
                    "Grouping by too many columns.",
                    "Missing requested COUNT/AVG/SUM metrics.",
                    "Using HVI unnecessarily for non-HVI questions.",
                    "Using RAG for exact database facts.",
                    "Forgetting the matched sensor subset limitation.",
                    "Using direct hvi.person_id = PEOPLE_170.study_person_id join incorrectly.",
                    "Returning valid SQL with wrong aliases or result columns.",
                    "Not checking whether the final output answers the exact wording.",
                ],
                "prevention_checklist": [
                    "Route the question to SQL, spatial, RAG, or combined evidence first.",
                    "Plan result shape before SQL.",
                    "Use known join templates when available.",
                    "Validate SQL for safety, then inspect result columns for analytical correctness.",
                    "Mention source type in final answer: SQL, spatial, or RAG chunks.",
                ],
            },
            indent=2,
        )
    if uri == "sqlite://sensor-analysis-rules":
        return json.dumps(
            {
                "source_of_truth": "Use SQL for exact sensor/AWS facts, not RAG.",
                "tables": {
                    "raw_rows": "survey.SENSOR_AWS_RESPONDENT_ROWS",
                    "respondent_summary": "survey.SENSOR_AWS_RESPONDENT_SUMMARY",
                    "urban_summary": "survey.SENSOR_AWS_URBAN_SUMMARY",
                },
                "joins": [
                    "SENSOR_AWS_RESPONDENT_ROWS joins through study_person_id.",
                    "HVI + sensor analysis must map hvi.hvi_scores to survey.PEOPLE_170 first, then join PEOPLE_170.study_person_id to sensor rows.",
                ],
                "matched_subset_limitation": "Only 7 HVI people have matched sensor rows; HVI + sensor answers must say they apply to the matched sensor subset.",
                "grain_choices": [
                    "sensor-row-level for raw timestamp analysis",
                    "study_person_id-level for person summaries",
                    "sensor_unique_id-level for device summaries",
                    "summary-level for pre-aggregated summary tables",
                ],
                "common_metrics": ["COUNT", "COUNT DISTINCT", "AVG temperature", "MAX temperature", "AVG humidity", "AVG heat index", "MAX heat index"],
            },
            indent=2,
        )
    if uri == "rag://rag-vs-sql-boundary":
        return json.dumps(
            {
                "server_role": "RAG tools return qualitative evidence chunks only; the client/LLM performs final synthesis.",
                "use_rag_for": [
                    "qualitative context from Word documents",
                    "narrative explanations of water, housing, livelihood, gender, land tenure, and lived heat experience",
                    "finding text evidence with chunk_id and file_name citations",
                ],
                "do_not_use_rag_for": [
                    "exact counts",
                    "HVI scores or categories",
                    "sensor temperature/humidity/heat-index values",
                    "coordinates",
                    "database-derived rankings or grouped summaries",
                ],
                "recommended_workflow": "Use SQL for exact quantitative facts, RAG for qualitative interpretation, then state which evidence source supports each final claim.",
            },
            indent=2,
        )
    if uri == "spatial://spatial-vs-sql-boundary":
        return json.dumps(
            {
                "server_role": "Spatial tools expose coordinate metadata and GeoJSON exports; the client/LLM decides when spatial output is needed.",
                "use_spatial_for": [
                    "spatial source discovery",
                    "coordinate completeness and validity",
                    "QGIS-ready GeoJSON point exports",
                    "spatial join readiness metadata",
                ],
                "do_not_use_spatial_for": [
                    "exact SQL aggregates",
                    "HVI score computation",
                    "sensor metric computation",
                    "qualitative narrative evidence",
                    "polygon joins or heavy QGIS processing in the current server",
                ],
                "recommended_workflow": "Use SQL to compute exact metrics; use spatial tools to export or inspect coordinate-ready layers.",
            },
            indent=2,
        )
    if uri == "spatial://parallel-qgis-mcp-workflow":
        return json.dumps(
            {
                "purpose": "Workflow for using this MCP server in parallel with a separate QGIS MCP server.",
                "server_roles": {
                    "this_mcp": "Query SQL databases, inspect spatial readiness, and export filtered point GeoJSON layers.",
                    "qgis_mcp": "Load GeoJSON layers, apply styles, run QGIS analysis where supported, and export maps.",
                },
                "workflow": [
                    "Use this MCP to inspect schema, joins, coordinate columns, and source limitations.",
                    "Use SQL tools to query respondent, HVI, sensor, water, or other survey data.",
                    "Use export_sql_points_geojson to convert a safe SELECT/WITH result with latitude/longitude columns into GeoJSON.",
                    "Pass the returned output_path to the QGIS MCP server.",
                    "Use QGIS MCP to load, style, analyze, and export maps.",
                ],
                "rules": [
                    "Do not expose raw database files to QGIS MCP; pass generated GeoJSON paths instead.",
                    "Do not claim polygon joins or polygon aggregation unless a valid polygon/multipolygon boundary layer is available.",
                    "Use SQL for exact counts and metrics before mapping.",
                    "Use RAG only for qualitative context, not coordinates or numeric facts.",
                    "Keep original databases read-only.",
                ],
                "recommended_tool": "export_sql_points_geojson",
            },
            indent=2,
        )
    if uri == "spatial://sql-to-geojson-guide":
        return json.dumps(
            {
                "purpose": "Guide for preparing SQL-derived point layers for QGIS.",
                "tool": "export_sql_points_geojson",
                "required_inputs": [
                    "sql: a read-only SELECT/WITH query",
                    "latitude_column: latitude column name in the SQL result",
                    "longitude_column: longitude column name in the SQL result",
                    "output_name: generated GeoJSON file name",
                ],
                "optional_inputs": [
                    "label_column: copied to _label property if present",
                    "limit: max rows to read from SQL result, default 1000",
                ],
                "output_location": "qgis_exports/generated/",
                "result_contract": [
                    "Validates SQL with existing validate_sql rules.",
                    "Executes with readonly_query.",
                    "Requires coordinate columns in the result.",
                    "Filters missing or invalid coordinates.",
                    "Writes an EPSG:4326 GeoJSON FeatureCollection.",
                    "Returns output_path, feature_count, coordinate columns, property fields, sample features, and warnings.",
                ],
                "example_use_cases": [
                    "Tanker-water respondents or households.",
                    "High HVI score people with coordinates.",
                    "Matched sensor subset summaries with coordinates.",
                    "Urban households matching a housing, water, or heat-risk condition.",
                ],
            },
            indent=2,
        )
    if uri == "rag://document-inventory":
        return json.dumps(load_json_file(RAG_DOCUMENT_INVENTORY_PATH), indent=2, ensure_ascii=False)
    if uri == "rag://chunking-plan":
        return json.dumps(load_json_file(RAG_CHUNKING_PLAN_PATH), indent=2, ensure_ascii=False)
    if uri == "rag://ingestion-report":
        return load_text_file(RAG_INGESTION_REPORT_PATH)
    if uri == "rag://search-readme":
        return load_text_file(RAG_SEARCH_README_PATH)
    if uri == "rag://corpus-summary":
        return json.dumps(rag_corpus_summary(), indent=2, ensure_ascii=False)
    if uri == "spatial://inventory":
        return json.dumps(load_spatial_inventory(), indent=2)
    if uri == "spatial://join-map":
        return json.dumps(load_spatial_join_map(), indent=2)
    if uri == "spatial://coordinate-quality":
        return json.dumps({"rows": load_coordinate_quality_rows()}, indent=2)
    if uri == "spatial://readiness-report":
        if not SPATIAL_READINESS_REPORT_PATH.exists():
            raise FileNotFoundError(f"Missing spatial readiness report: {SPATIAL_READINESS_REPORT_PATH.name}")
        return SPATIAL_READINESS_REPORT_PATH.read_text(encoding="utf-8")
    if uri == "sqlite://join-map":
        return json.dumps({"known_joins": KNOWN_JOINS}, indent=2)
    if uri == "sqlite://sql-safety-rules":
        rules = [
            "Read-only only.",
            "Use SELECT or WITH only.",
            "No schema modification.",
            "No INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, REPLACE, ATTACH, DETACH, PRAGMA, VACUUM, or transaction control.",
            "Use already attached aliases survey. and hvi.; do not attach databases from client SQL.",
            "Always qualify tables with survey. or hvi. for cross-database queries.",
            "Prefer LIMIT for exploration.",
        ]
        return json.dumps({"rules": rules}, indent=2)
    if uri == "sqlite://database-summary":
        return json.dumps(DATABASE_SUMMARY, indent=2)
    if uri == "sqlite://sql-agent-result-shape-guide":
        return json.dumps(
            {
                "rules": [
                    "For exact_number questions, return count-style columns and avoid unnecessary detail columns.",
                    "For grouped_summary questions, group only by fields named or implied in the question.",
                    "For comparison questions, return one row per comparison group with aggregate metrics.",
                    "For ranked_list questions, include ORDER BY and LIMIT.",
                    "For validation_summary questions, include both tested condition and validation result/count metrics.",
                    "Do not add extra GROUP BY columns just because they are available.",
                    "Column aliases should describe the metric clearly, e.g. matched_sensor_people, pct_with_sensor_rows, avg_hvi_score.",
                ]
            },
            indent=2,
        )
    if uri == "sqlite://hvi-survey-join-rules":
        return json.dumps(
            {
                "do_not_use": [
                    "hvi.hvi_scores.person_id = survey.PEOPLE_170.study_person_id"
                ],
                "correct_join": (
                    "ON ((lower(h.source_db) = 'main' AND h.person_id = p.main_respondent_id) "
                    "OR (lower(h.source_db) = 'urban' AND CAST(h.person_id AS TEXT) = CAST(p.urban_household_id AS TEXT)))"
                ),
                "verified_counts": {
                    "total_hvi_matched_to_people_170": 140,
                    "main_matched": 29,
                    "urban_matched": 111,
                    "hvi_people_with_sensor_rows": 7,
                    "matched_sensor_rows": 32913,
                },
                "notes": [
                    "Use source-dependent join for every HVI + survey query.",
                    "Use PEOPLE_170.study_person_id only after mapping HVI to PEOPLE_170.",
                ],
            },
            indent=2,
        )
    if uri == "sqlite://sensor-subset-rules":
        return json.dumps(
            {
                "scope": "Only 7 HVI people have matched sensor rows.",
                "required_path": "hvi.hvi_scores -> survey.PEOPLE_170 -> survey.SENSOR_AWS_RESPONDENT_ROWS",
                "known_thresholds": {
                    "sensor_temperature_c_high": ">= 35",
                    "sensor_humidity_rh_high": ">= 70",
                    "aws_heat_index_c_high": ">= 40",
                },
                "rules": [
                    "Always state that HVI + sensor answers apply only to the matched sensor subset.",
                    "For hourly analysis, use substr(s.aws_timestamp, 12, 2).",
                    "For date analysis, use s.aws_date_key.",
                    "For monthly analysis, use s.aws_month_key if available.",
                    "Use COUNT(DISTINCT h.person_id) to report number of HVI people represented.",
                ],
            },
            indent=2,
        )
    if uri == "sqlite://challenge-sql-lessons":
        return json.dumps(
            {
                "observed_failures_from_step_7b": [
                    "Valid SQL can still fail result-level comparison if result shape differs.",
                    "Over-granular grouping caused mismatches.",
                    "Different binning rules caused mismatches.",
                    "Missing requested metrics caused mismatches.",
                    "Top-k/order mismatches caused failures.",
                    "Aliases and column names matter for evaluation readability.",
                ],
                "fixes": [
                    "Plan expected columns before writing SQL.",
                    "Group only at the level requested by the question.",
                    "Use documented thresholds and bands.",
                    "Include every requested metric in SELECT.",
                    "Use ORDER BY on the metric that defines top/highest/hottest/most.",
                ],
            },
            indent=2,
        )
    if uri == "sqlite://challenge-failure-refinement-rules":
        return json.dumps(
            {
                "purpose": (
                    "Failure-targeted guidance for hard SQL benchmark traps. Use these definitions during "
                    "result-shape planning and self-check before validate_sql."
                ),
                "join_comparison_output_shape": {
                    "columns": ["join_type", "matched_rows"],
                    "rows": [
                        {
                            "join_type": "direct_person_id_to_study_person_id",
                            "definition": "COUNT rows from hvi.hvi_scores h JOIN survey.PEOPLE_170 p ON h.person_id = p.study_person_id",
                        },
                        {
                            "join_type": "source_dependent_join",
                            "definition": "COUNT rows using the documented source-dependent HVI-to-PEOPLE_170 join template",
                        },
                    ],
                    "notes": [
                        "Do not rename the comparison labels.",
                        "Use UNION ALL to keep one row per join comparison.",
                    ],
                },
                "top_quartile_sensor_coverage_shape": {
                    "columns": [
                        "hvi_quartile",
                        "people",
                        "people_without_sensor_rows",
                        "people_with_sensor_rows",
                    ],
                    "quartile_rule": "NTILE(4) OVER (ORDER BY hvi_answer_level_score DESC); top quartile is hvi_quartile = 1.",
                    "sensor_rule": "Map HVI people to PEOPLE_170 first, then LEFT JOIN distinct SENSOR_AWS_RESPONDENT_ROWS.study_person_id.",
                    "notes": [
                        "Return only the top-quartile aggregate row for this shape.",
                        "Count people at the mapped-person level, not raw sensor rows.",
                    ],
                },
                "family_risk_threshold_rules": {
                    "source_scope": "main respondents only for main-survey family vulnerability questions.",
                    "elderly_members": "SUM(CASE WHEN CAST(AGE AS REAL) >= 60 THEN 1 ELSE 0 END)",
                    "young_children": "SUM(CASE WHEN CAST(AGE AS REAL) < 6 THEN 1 ELSE 0 END)",
                    "labels": {
                        "elderly_band": ["has_elderly", "no_elderly"],
                        "child_band": ["has_young_children", "no_young_children"],
                    },
                    "columns": [
                        "elderly_band",
                        "child_band",
                        "main_respondents",
                        "avg_hvi_score",
                        "avg_sensitivity_score",
                    ],
                    "order_by": "avg_sensitivity_score DESC, main_respondents DESC",
                },
                "cooling_asset_and_unmet_demand_definitions": {
                    "source_scope": "main respondents only for RESPONDENT_COOLING_APPLIANCES questions.",
                    "cooling_asset_status": {
                        "has_some_cooling_asset": "COALESCE(FAN,'') <> '' OR COALESCE(AIR_COOLER,'') <> '' OR COALESCE(FRIDGE,'') <> ''",
                        "no_listed_cooling_asset": "none of FAN, AIR_COOLER, or FRIDGE is non-empty",
                    },
                    "unmet_cooling_demand": {
                        "reports_unmet_cooling_demand": "COALESCE(AIR_CONDITIONER_2,'') <> '' OR COALESCE(AIR_COOLER_2,'') <> '' OR COALESCE(GREEN_COVER_2,'') <> ''",
                        "no_listed_unmet_demand": "none of AIR_CONDITIONER_2, AIR_COOLER_2, or GREEN_COVER_2 is non-empty",
                    },
                    "columns": [
                        "cooling_asset_status",
                        "unmet_cooling_demand",
                        "main_respondents",
                        "avg_lack_adaptive_capacity",
                        "avg_hvi_score",
                    ],
                    "order_by": "avg_lack_adaptive_capacity DESC, main_respondents DESC",
                },
                "codebook_vs_evidence_risk_gap": {
                    "join_keys": ["hvi_domain", "indicator_group"],
                    "include_filter": "lower(CAST(include_in_hvi AS TEXT)) IN ('true','1','yes')",
                    "columns": [
                        "hvi_domain",
                        "indicator_group",
                        "codebook_rows",
                        "evidence_rows",
                        "avg_codebook_risk",
                        "avg_evidence_risk",
                        "absolute_risk_gap",
                    ],
                    "risk_gap_definition": "ROUND(ABS(avg_codebook_risk - avg_evidence_risk), 4)",
                    "order_by": "absolute_risk_gap DESC, evidence_rows DESC",
                    "limit": 20,
                    "notes": [
                        "Keep codebook and evidence averages as separate aliases.",
                        "Do not call the gap column risk_gap if the requested shape expects absolute_risk_gap.",
                    ],
                },
                "street_width_and_floor_bands": {
                    "source_scope": "urban HVI households joined to URBAN_RESPONDENTS.",
                    "street_width_band": [
                        {"label": "narrow_lt_2m", "rule": "CAST(width_of_the_street_metres AS REAL) < 2"},
                        {"label": "medium_2_to_4m", "rule": "CAST(width_of_the_street_metres AS REAL) < 4"},
                        {"label": "wide_4m_plus", "rule": "otherwise"},
                    ],
                    "floor_band": [
                        {"label": "one_floor", "rule": "CAST(no_of_floors_in_the_building AS REAL) <= 1"},
                        {"label": "two_to_three_floors", "rule": "CAST(no_of_floors_in_the_building AS REAL) <= 3"},
                        {"label": "four_plus_floors", "rule": "otherwise"},
                    ],
                    "columns": [
                        "street_width_band",
                        "floor_band",
                        "households",
                        "avg_exposure",
                        "avg_hvi_score",
                    ],
                    "order_by": "avg_exposure DESC, households DESC",
                },
            },
            indent=2,
        )
    raise ValueError(f"Unknown resource URI: {uri}")


def prompt_payload(name: str) -> dict[str, Any]:
    prompts = {
        "complex_sql_analysis": (
            "Inspect schema first. Identify required database aliases and tables. Discover or verify join keys. "
            "Draft SQL using survey. and hvi. qualified table names. Validate SQL before execution. "
            "Use EXPLAIN QUERY PLAN when joins or filters are complex. Execute the SQL. Answer only from the SQL result."
        ),
        "sql_debugging": (
            "If SQL fails, inspect the schema again. Check database aliases, table names, column names, case, and known join keys. "
            "Rewrite the query, validate it, and rerun. Prefer small LIMITs while debugging."
        ),
        "answer_from_sql_only": (
            "Answer only from returned SQL results. Do not invent names, scores, sensor values, categories, or counts. "
            "If the result is empty, say the database returned no matching rows."
        ),
        "challenge_sql_analysis": (
            "Use this prompt for hard SQL benchmark questions. Before SQL: classify expected answer type; decide exact result shape "
            "including columns, grouping level, metrics, order, and limit; identify required tables and known join templates; identify "
            "thresholds and bands without inventing them unless documented. Use CTEs for multi-step logic. Use the source-dependent "
            "hvi_scores_to_people_170 join for HVI + survey queries. For sensor questions, restrict to the matched sensor subset. "
            "Avoid over-granular GROUP BY. Include every metric requested by the question. Self-check before execution, then validate_sql, "
            "optionally explain_sql_plan, readonly_query, and answer from results only."
        ),
        "result_shape_self_check": (
            "Review generated SQL before execution. Return PASS only if SELECT columns match the expected answer type, GROUP BY level "
            "matches the question wording, requested metrics are present, ORDER BY and LIMIT are correct for ranked questions, HVI + "
            "survey joins use the source-dependent template, and HVI + sensor questions are limited to the matched sensor subset. "
            "If any check fails, rewrite SQL before validation."
        ),
        "sensor_subset_sql": (
            "For HVI + sensor questions: first map HVI people to PEOPLE_170 using the source-dependent join, then join PEOPLE_170.study_person_id "
            "to SENSOR_AWS_RESPONDENT_ROWS.study_person_id or SENSOR_AWS_RESPONDENT_SUMMARY.study_person_id. Include COUNT(DISTINCT h.person_id) "
            "where reporting representation. State that the result applies only to the matched sensor subset. Use documented thresholds: "
            "sensor_temperature_c >= 35, sensor_humidity_rh >= 70, aws_heat_index_c >= 40."
        ),
        "failure_targeted_sql_refinement": (
            "For known hard challenge traps, read sqlite://challenge-failure-refinement-rules before SQL generation. "
            "Match requested output labels exactly; use documented thresholds and bands; keep aggregate result shapes at the requested granularity; "
            "and self-check aliases for join comparisons, top-quartile counts, family risk, cooling assets, codebook/evidence gaps, and urban bands."
        ),
        "spatial_export_planning": (
            "Plan lightweight point exports only. Start with list_spatial_sources, then describe_spatial_source and coordinate_quality_report. "
            "Use export_points_geojson only for supported ready coordinate-bearing sources. Filter missing coordinates, assume EPSG:4326, "
            "and avoid map styling or heavy QGIS processing."
        ),
        "coordinate_quality_check": (
            "Before exporting or mapping, check complete coordinate counts, missing latitude/longitude, coordinate ranges, zero-zero coordinates, "
            "and invalid global ranges. Treat missing coordinates as a cleaning issue and report them explicitly."
        ),
        "sensor_point_export_planning": (
            "For sensor points, use survey.SENSOR_AWS_RESPONDENT_ROWS, filter rows with respondent_latitude/respondent_longitude, "
            "and aggregate repeated readings to one feature per study_person_id by default. Include sensor row counts, temperature/humidity/heat-index summaries, "
            "and first/last timestamps."
        ),
        "prepare_layer_for_qgis_mcp": (
            "Use this prompt when the user wants a filtered layer for mapping in a separate QGIS MCP server. "
            "First decide the SQL result shape and identify latitude/longitude columns. Use SQL tools to inspect schema if needed, then write a read-only SELECT/WITH query "
            "that returns one point row per respondent, household, sensor summary, or other intended feature. Include useful map properties such as IDs, names, categories, scores, "
            "or source labels. Validate SQL and call export_sql_points_geojson with the coordinate columns, output_name, optional label_column, and a safe limit. "
            "Return the GeoJSON output_path and feature_count to the user, and say that the QGIS MCP server can load that file for styling and map export. "
            "Do not claim polygon joins or aggregation unless a valid polygon/multipolygon boundary layer is available. Do not modify databases or expose raw database files."
        ),
        "rag_retrieval_planning": (
            "Use local RAG retrieval for qualitative writing context only. Start with rag_corpus_summary or list_rag_documents, "
            "then call search_rag_chunks with a concise keyword query and optional filters. Retrieve full text with get_rag_chunk only for selected evidence chunks. "
            "Do not use these tools for SQL-derived HVI scores, sensor values, or spatial facts."
        ),
        "qualitative_context_lookup": (
            "For questions about qualitative themes such as water, land tenure, housing materials, livelihood, gender, or heat experience, "
            "use search_rag_chunks and metadata filters. Prefer returning chunk IDs, file names, and previews as evidence context."
        ),
        "rag_answer_from_chunks_only": (
            "If asked to synthesize an answer, use only retrieved chunks and cite chunk_id and file_name. "
            "Do not invent claims, do not use these chunks as numerical database evidence, and say when retrieval returned no relevant chunks."
        ),
        "choose_analysis_layer": (
            "Before calling tools, classify the user request as SQL, spatial, RAG, or combined. "
            "The MCP server does not answer questions by itself; the MCP client/LLM performs reasoning and final answer generation. "
            "Use SQL for exact database facts, counts, averages, rankings, grouped summaries, HVI/survey/sensor analysis, and validation. "
            "Use spatial tools for coordinate quality, spatial source discovery, GeoJSON export, and QGIS-ready layers. "
            "Use RAG for qualitative narrative/document context from Word documents. "
            "Do not use RAG for exact counts, HVI scores, sensor values, or coordinates. "
            "Do not use HVI tables unless the question asks about HVI scores, categories, indicators, vulnerability, or HVI evidence. "
            "For combined answers, state which evidence came from SQL, spatial tools, and RAG chunks."
        ),
        "plan_sql_result_shape": (
            "Before writing SQL, explicitly plan: expected_answer_type "
            "(exact_number, grouped_summary, ranked_list, comparison, row_level_listing, explanation); expected output columns; "
            "grouping level; metrics needed (COUNT, AVG, SUM, MIN, MAX, percentage); filters; joins; ORDER BY and LIMIT; "
            "and whether the answer should be respondent-level, family-member-level, household-level, sensor-row-level, or summary-level. "
            "Use RESPONDENT_* tables by RESPONDENT_ID or RESPONDENT_UUID, FAMILY_MEMBER_* through RESPONDENT_UUID/SUBMISSION_UUID, "
            "URBAN_* by household_id, SENSOR_AWS_RESPONDENT_ROWS by study_person_id, and HVI to PEOPLE_170 only with the source-dependent join template. "
            "PEOPLE_170 is a bridge table, not always the analysis anchor. Avoid HVI for non-HVI questions. "
            "After SQL generation, validate_sql checks safety only; still self-check whether result columns answer the exact wording."
        ),
        "answer_with_sql_spatial_rag": (
            "Use this when a final answer may combine quantitative SQL, spatial export information, and qualitative RAG context. "
            "Ground exact counts, scores, averages, rankings, sensor values, and coordinates in SQL or spatial tool outputs, not RAG. "
            "Ground qualitative interpretations in retrieved RAG chunks with chunk_id and file_name. "
            "Mention the source type for each major claim: SQL result, spatial tool/export, or RAG chunk. "
            "If one source was not used, say so only if relevant. Do not invent values or infer unsupported relationships."
        ),
        "avoid_common_sql_failures": (
            "Before executing generated SQL, check for common failures: returning detail rows when grouped summary is needed; "
            "GROUP BY includes too many columns; missing requested COUNT/AVG/SUM/MIN/MAX/percentage metrics; unnecessary HVI joins; "
            "RAG used for exact database facts; matched sensor subset limitation omitted; incorrect direct hvi.person_id = PEOPLE_170.study_person_id join; "
            "wrong aliases/result columns; final output does not answer the exact wording. "
            "If any issue appears, rewrite the SQL before validate_sql or readonly_query."
        ),
        "final_answer_grounding_check": (
            "Before final answer, verify every claim is grounded. SQL results support exact database facts, counts, averages, rankings, grouped summaries, "
            "HVI scores/categories, and sensor metrics. Spatial tools support coordinate quality, source readiness, feature counts, output paths, and QGIS-ready exports. "
            "RAG chunks support qualitative narrative context only and should cite chunk_id and file_name. "
            "State limitations such as matched sensor subset when relevant. If returned results are empty, say the tool/database returned no matching rows or chunks."
        ),
        "unsupported_question_redirect": (
            "Use this prompt when the user asks an unsupported or unsafe question. The MCP server only supports questions grounded in connected SQL databases, "
            "spatial/QGIS outputs, and RAG document chunks. The MCP server does not predict individual death, medical outcomes, future disasters, or live weather. "
            "The MCP client/LLM performs reasoning and final answer generation; the server provides safe tools and guardrails. "
            "If asked 'who will die' or a similar individual mortality prediction, do not call tools to predict death. State that the system cannot predict death, "
            "then offer a safe heat vulnerability/risk indicator analysis using SQL, such as highest HVI scores/categories, high heat exposure in the matched sensor subset, "
            "or age, illness, housing, and water-access risk indicators. If asked 'what is tomorrow's weather', state that no live weather API is connected and offer historical "
            "sensor/AWS analysis from the database instead. Use SQL for exact database facts; spatial/QGIS tools for coordinates, GeoJSON, and map outputs; and RAG for qualitative "
            "document context only. Do not use RAG for exact counts, HVI scores, sensor values, or coordinates. In final answers, say whether evidence came from SQL, spatial/QGIS, "
            "or RAG chunks."
        ),
        "qgis_map_planning": (
            "Use QGIS tools only for spatial visualization or spatial joins. First call qgis_check_environment. "
            "If PyQGIS is unavailable, do not fake map generation; use QGIS-ready GeoJSON exports and report the limitation. "
            "Use SQL tools for exact numeric summaries before mapping. Use RAG tools only for qualitative map interpretation context. "
            "Always report CRS assumptions, layers used, and layer limitations."
        ),
        "qgis_style_selection": (
            "Choose categorized styling for text/category fields such as hvi_category, place, or source_db. "
            "Choose graduated styling for numeric fields such as hvi_answer_level_score, avg_sensor_temperature_c, "
            "avg_aws_heat_index_c, or max_sensor_temperature_c. Use SQL to compute exact values before mapping. "
            "Do not imply a style changes the underlying data. Report CRS assumptions and layer limitations."
        ),
        "qgis_polygon_join_planning": (
            "Use polygon joins only when list_polygon_boundary_sources finds a usable polygon or multipolygon boundary. "
            "Validate point and polygon layer existence, geometry type, CRS compatibility, and output path before joining. "
            "Do not claim polygon aggregation unless polygon boundary data was used. Use SQL tools for exact numeric summaries before or after the join, "
            "and use RAG tools only for qualitative interpretation context."
        ),
    }
    if name not in prompts:
        raise ValueError(f"Unknown prompt: {name}")
    return {
        "description": next(prompt["description"] for prompt in PROMPTS if prompt["name"] == name),
        "messages": [
            {
                "role": "user",
                "content": {"type": "text", "text": prompts[name]},
            }
        ],
    }


def call_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    handlers = {
        "list_databases": lambda: list_databases(),
        "health_check": health_check,
        "list_tables": lambda: list_tables(args["database_alias"]),
        "get_schema_overview": lambda: get_schema_overview(args["database_alias"]),
        "describe_table": lambda: describe_table(args["database_alias"], args["table_name"]),
        "get_table_sample": lambda: get_table_sample(args["database_alias"], args["table_name"], args.get("limit", 5)),
        "get_row_count": lambda: get_row_count(args["database_alias"], args["table_name"]),
        "get_column_profile": lambda: get_column_profile(args["database_alias"], args["table_name"], args["column_name"]),
        "find_common_columns": lambda: find_common_columns(
            args["database_alias_a"], args["table_a"], args["database_alias_b"], args["table_b"]
        ),
        "get_known_join_map": get_known_join_map,
        "get_join_template": lambda: get_join_template(args["join_name"]),
        "validate_sql": lambda: {"ok": True, **validate_sql_text(args["sql"])},
        "explain_sql_plan": lambda: explain_sql_plan(args["sql"]),
        "readonly_query": lambda: readonly_query(args["sql"], args.get("limit", DEFAULT_QUERY_LIMIT)),
        "run_join_verification": run_join_verification,
        "list_spatial_sources": list_spatial_sources,
        "describe_spatial_source": lambda: describe_spatial_source(args["source_name"]),
        "coordinate_quality_report": lambda: coordinate_quality_report(args.get("source_name")),
        "export_points_geojson": lambda: export_points_geojson(
            args["source_name"],
            args.get("output_path"),
            args.get("limit"),
            bool(args.get("overwrite", False)),
            bool(args.get("include_geojson", False)),
        ),
        "export_sql_points_geojson": lambda: export_sql_points_geojson(
            args["sql"],
            args["latitude_column"],
            args["longitude_column"],
            args["output_name"],
            args.get("label_column"),
            args.get("limit", 1000),
        ),
        "export_sensor_matched_points_geojson": lambda: export_sensor_matched_points_geojson(
            args.get("group_by", "study_person_id"),
            args.get("output_path"),
            bool(args.get("overwrite", False)),
            bool(args.get("include_geojson", False)),
        ),
        "get_spatial_join_map": get_spatial_join_map,
        "qgis_check_environment": qgis_check_environment,
        "qgis_load_geojson_layer": lambda: qgis_load_geojson_layer(args["path"], args.get("layer_name")),
        "qgis_create_project": lambda: qgis_create_project(args["project_path"], args.get("layer_paths")),
        "qgis_apply_categorized_style": lambda: qgis_apply_categorized_style(args["layer_name"], args["field_name"], args.get("categories")),
        "qgis_apply_graduated_style": lambda: qgis_apply_graduated_style(
            args["layer_name"], args["field_name"], args.get("classes", 5), args.get("mode", "quantile")
        ),
        "qgis_export_map_png": lambda: qgis_export_map_png(
            args.get("project_path", ""), args["output_path"], args.get("title"), args.get("width", 1600), args.get("height", 1200)
        ),
        "qgis_export_map_pdf": lambda: qgis_export_map_pdf(args.get("project_path", ""), args["output_path"], args.get("title")),
        "list_polygon_boundary_sources": list_polygon_boundary_sources,
        "qgis_polygon_join_points_to_boundary": lambda: qgis_polygon_join_points_to_boundary(
            args["point_layer_path"], args["polygon_layer_path"], args["output_path"], args.get("summary_fields")
        ),
        "list_rag_documents": list_rag_documents,
        "search_rag_chunks": lambda: search_rag_chunks(args["query"], args.get("top_k", 5), args.get("filters")),
        "get_rag_chunk": lambda: get_rag_chunk(args["chunk_id"]),
        "filter_rag_chunks": lambda: filter_rag_chunks(
            args.get("file_name"),
            args.get("document_category"),
            args.get("topic_tags"),
            args.get("content_type"),
            args.get("limit", 20),
        ),
        "rag_corpus_summary": rag_corpus_summary,
    }
    if name not in handlers:
        raise ValueError(f"Unknown tool: {name}")
    return response_json(handlers[name]())


def handle_request(request: dict[str, Any]) -> dict[str, Any] | None:
    method = request.get("method")
    request_id = request.get("id")
    params = request.get("params") or {}
    if request_id is None:
        return None
    try:
        if method == "initialize":
            result = {
                "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                "capabilities": {
                    "tools": {},
                    "resources": {},
                    "prompts": {},
                },
                "serverInfo": {"name": "sqlite-readonly-sql-reasoning", "version": "0.1.0"},
            }
        elif method == "tools/list":
            result = {"tools": TOOL_SCHEMAS}
        elif method == "tools/call":
            result = call_tool(params["name"], params.get("arguments") or {})
        elif method == "resources/list":
            result = {
                "resources": [
                    {
                        "uri": "mcp://analysis-routing-guide",
                        "name": "Analysis Routing Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "mcp://unsupported-question-policy",
                        "name": "Unsupported Question Policy",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "mcp://safety-and-scope-policy",
                        "name": "Safety and Scope Policy",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://join-map",
                        "name": "Known SQLite Join Map",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://sql-safety-rules",
                        "name": "SQL Safety Rules",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://database-summary",
                        "name": "Database Summary",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://sql-agent-result-shape-guide",
                        "name": "SQL Agent Result Shape Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://hvi-survey-join-rules",
                        "name": "HVI Survey Join Rules",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://sensor-subset-rules",
                        "name": "Sensor Subset Rules",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://challenge-sql-lessons",
                        "name": "Challenge SQL Lessons",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://challenge-failure-refinement-rules",
                        "name": "Challenge Failure Refinement Rules",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://field-survey-table-family-guide",
                        "name": "Field Survey Table Family Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://result-shape-planning-guide",
                        "name": "Result Shape Planning Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://agent-failure-lessons",
                        "name": "Agent Failure Lessons",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "sqlite://sensor-analysis-rules",
                        "name": "Sensor Analysis Rules",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://inventory",
                        "name": "Spatial Source Inventory",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://join-map",
                        "name": "Spatial Join Map",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://coordinate-quality",
                        "name": "Coordinate Quality Report",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://readiness-report",
                        "name": "Spatial Readiness Report",
                        "mimeType": "text/markdown",
                    },
                    {
                        "uri": "spatial://spatial-vs-sql-boundary",
                        "name": "Spatial vs SQL Boundary",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://parallel-qgis-mcp-workflow",
                        "name": "Parallel QGIS MCP Workflow",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "spatial://sql-to-geojson-guide",
                        "name": "SQL to GeoJSON Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "qgis://environment",
                        "name": "QGIS Environment",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "qgis://standard-layer-style-guide",
                        "name": "QGIS Standard Layer Style Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "qgis://map-export-guide",
                        "name": "QGIS Map Export Guide",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "qgis://polygon-join-requirements",
                        "name": "QGIS Polygon Join Requirements",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "rag://document-inventory",
                        "name": "RAG Document Inventory",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "rag://chunking-plan",
                        "name": "RAG Chunking Plan",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "rag://ingestion-report",
                        "name": "RAG Ingestion Report",
                        "mimeType": "text/markdown",
                    },
                    {
                        "uri": "rag://search-readme",
                        "name": "RAG Search README",
                        "mimeType": "text/markdown",
                    },
                    {
                        "uri": "rag://corpus-summary",
                        "name": "RAG Corpus Summary",
                        "mimeType": "application/json",
                    },
                    {
                        "uri": "rag://rag-vs-sql-boundary",
                        "name": "RAG vs SQL Boundary",
                        "mimeType": "application/json",
                    },
                ]
            }
        elif method == "resources/read":
            uri = params["uri"]
            mime_type = "text/markdown" if uri in {"spatial://readiness-report", "rag://ingestion-report", "rag://search-readme"} else "application/json"
            result = {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": mime_type,
                        "text": resource_payload(uri),
                    }
                ]
            }
        elif method == "prompts/list":
            result = {"prompts": PROMPTS}
        elif method == "prompts/get":
            result = prompt_payload(params["name"])
        else:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
        return {"jsonrpc": "2.0", "id": request_id, "result": result}
    except Exception as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": str(exc)},
        }


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
            response = handle_request(request)
        except Exception as exc:
            response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": str(exc)},
            }
        if response is not None:
            print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
