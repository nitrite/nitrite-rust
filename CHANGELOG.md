# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-12-15

### Added

- **Core Database** (`nitrite`)
  - Document-oriented embedded database for Rust
  - ACID transactions with optimistic locking
  - Flexible indexing: unique, non-unique, and compound indexes
  - Rich query API with fluent filter builders
  - Document and object repository patterns
  - Schema migration support

- **Derive Macros** (`nitrite-derive`)
  - `#[derive(NitriteEntity)]` for automatic entity mapping
  - `#[derive(Convertible)]` for document serialization
  - Attribute macros for ID fields and indexes

- **Storage Backend** (`nitrite-fjall-adapter`)
  - Fjall LSM-tree based persistent storage
  - Configurable with bincode or bitcode serialization
  - High-performance disk-backed storage

- **Full-Text Search** (`nitrite-tantivy-fts`)
  - Tantivy-powered FTS integration
  - Phrase search, fuzzy matching, wildcards
  - Configurable tokenizers and analyzers

- **Spatial Indexing** (`nitrite-spatial`)
  - R-tree based spatial index implementation
  - Disk-persistent R-tree with crash recovery
  - Range and nearest-neighbor queries

- **Testing & Benchmarks**
  - Comprehensive integration test suite
  - Performance benchmarks comparing with SQLite and Redb

### Notes

- Initial release
- Minimum supported Rust version: 1.70+
