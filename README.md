# MCP-Based Research Support System for Urban Heat Vulnerability Analysis

An AI-powered research support system that integrates **Model Context Protocol (MCP), Retrieval-Augmented Generation (RAG), natural-language SQL, and GIS tools** to support urban heat vulnerability research.

The system connects structured datasets, research documents, and spatial information, allowing users to investigate research questions through natural language.

## Key Features

- **Agentic AI and MCP:** Connects an LLM to external tools for database queries, document retrieval, and spatial analysis.
- **SQL Agent:** Converts natural-language questions into SQL queries to retrieve structured evidence from databases.
- **Document RAG:** Retrieves relevant passages from research documents to support evidence-grounded answers.
- **Chunking Strategy Evaluation:** Compares fixed-size, recursive, semantic, structural, and LLM-based document chunking.
- **GIS and Spatial Analysis:** Integrates geographic datasets and QGIS-based operations into the research workflow.
- **Evaluation Framework:** Assesses retrieval quality, answer generation, tool selection, execution reliability, and end-to-end task correctness.

## System Architecture

```text
Natural-Language Research Question
                 |
                 v
          LLM / AI Agent
                 |
        MCP Tool Orchestration
                 |
       +---------+---------+
       |         |         |
       v         v         v
    SQL MCP   Document   GIS / QGIS
              RAG/BM25      MCP
       |         |         |
       v         v         v
 Structured   Retrieved   Spatial
 Evidence     Passages    Evidence
       |         |         |
       +---------+---------+
                 |
                 v
       Evidence Integration
                 |
                 v
        Research-Oriented Answer
```

## RAG Evaluation

The project investigates how document chunking strategies affect retrieval and answer quality.

Strategies evaluated:
- Fixed-size chunking
- Recursive chunking
- Semantic chunking
- Structural chunking
- LLM-based chunking

Evaluation metrics include:
- Recall@K and Precision@K
- Mean Reciprocal Rank (MRR)
- Normalized Discounted Cumulative Gain (nDCG)
- Faithfulness
- Answer relevancy
- Context precision and context recall

**RAGAS** can be used for reference-based and LLM-based RAG evaluation, with the exact metrics depending on the implemented evaluation pipeline.

## MCP Evaluation

The MCP evaluation benchmark focuses on the reliability of tool-augmented research workflows:

- Tool selection and routing
- SQL generation and execution
- Document retrieval
- Spatial tool execution
- Tool-output validation
- End-to-end task correctness

## Research Data

The intended research workflow combines multiple evidence sources:

- Household survey records
- AcuRite weather-station observations
- Landsat-derived environmental indicators
- OpenStreetMap data and QGIS layers
- Research papers and policy documents

These sources support investigation of heat exposure and vulnerability in informal urban settlements.

## Technology Stack

**Languages and data:** Python, SQL, SQLite, Pandas

**AI and retrieval:** LLMs, RAG, BM25, agentic workflows

**Tool integration:** Model Context Protocol (MCP)

**GIS:** QGIS, spatial data processing

**Evaluation:** Retrieval metrics, RAGAS metrics where implemented, and MCP workflow validation

**Backend:** FastAPI, where used for API integration

## Project Goals

1. Enable natural-language access to structured, document-based, and spatial research evidence.
2. Compare document chunking strategies using consistent RAG evaluation criteria.
3. Evaluate the reliability of MCP-based tool execution across a complete research workflow.
4. Demonstrate how agentic AI can support reproducible, evidence-based urban heat vulnerability analysis.

## Repository Structure

```text
MCP_RAG/
├── Chunking_strategies.ipynb
├── Document_RAG.ipynb
├── Database_RAG.ipynb
├── main.py
├── server.py
├── Results.pdf
├── SQL_generated.pdf
└── README.md
```

## Intended Outcome

A modular AI research assistant that combines structured queries, document retrieval, and spatial analysis to make complex urban climate research workflows more accessible, traceable, and evaluable.
