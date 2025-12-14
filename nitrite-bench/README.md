# Nitrite Benchmark Suite

Comprehensive benchmarks for Nitrite database covering CRUD operations, indexing, spatial queries, full-text search, concurrency, transactions, and database comparisons.

## Benchmark Categories

| Category | Description |
|----------|-------------|
| **CRUD** | Insert, read, update, delete operations |
| **Indexing** | Index creation and indexed search |
| **Spatial** | Spatial index with bounding box and proximity queries |
| **FTS** | Tantivy-based full-text search indexing and queries |
| **Concurrency** | Multi-threaded insert, read, and mixed workloads |
| **Transactions** | Commit, rollback, and multi-operation transactions |
| **Comparison** | Performance comparison with SQLite, Redb, Sled (optional) |

## Running Benchmarks

### Quick Validation (Fast)

Test that benchmarks compile and run without full measurement:

```bash
cargo bench -p nitrite_bench -- --test
```

### Full Benchmark Suite

Run all benchmarks with HTML report generation:

```bash
cargo bench -p nitrite_bench
```

### Run Specific Benchmark Category

```bash
# CRUD benchmarks only
cargo bench -p nitrite_bench --bench crud_bench

# Index benchmarks only
cargo bench -p nitrite_bench --bench index_bench

# Spatial benchmarks only
cargo bench -p nitrite_bench --bench spatial_bench

# Full-text search benchmarks only
cargo bench -p nitrite_bench --bench fts_bench

# Concurrency benchmarks only
cargo bench -p nitrite_bench --bench concurrency_bench

# Transaction benchmarks only
cargo bench -p nitrite_bench --bench transaction_bench

# Database comparison (requires feature)
cargo bench -p nitrite_bench --features comparison --bench comparison_bench
```

### Filter by Benchmark Name

```bash
# Run only insert benchmarks
cargo bench -p nitrite_bench -- insert

# Run only fjall store benchmarks
cargo bench -p nitrite_bench -- fjall

# Run only in-memory benchmarks
cargo bench -p nitrite_bench -- inmemory
```

## Database Comparison Benchmarks

Compare Nitrite against other embedded databases:

| Database | Type | Notes |
|----------|------|-------|
| SQLite | Relational | Widely used embedded database |
| Redb | Key-Value | Modern LMDB-inspired database for Rust |
| Sled | Key-Value | Pure Rust embedded database |

```bash
# Run comparison benchmarks (downloads dependencies on first run)
cargo bench -p nitrite_bench --features comparison --bench comparison_bench
```

## Viewing HTML Reports

After running benchmarks, open the generated HTML report:

```bash
# macOS
open target/criterion/report/index.html

# Linux
xdg-open target/criterion/report/index.html

# Windows
start target/criterion/report/index.html
```

The report includes:
- Performance graphs with trend visualization
- Statistical analysis (mean, median, standard deviation)
- Distribution plots
- Detailed timing breakdowns

## Baseline Comparison (Release Comparison)

### Save Baseline for Current Release

```bash
cargo bench -p nitrite_bench -- --save-baseline v0.1.0
```

### Compare Against Previous Release

```bash
# Run benchmarks and compare against saved baseline
cargo bench -p nitrite_bench -- --baseline v0.1.0
```

### Compare Two Saved Baselines

```bash
# First save both baselines
cargo bench -p nitrite_bench -- --save-baseline old-version
# Make changes...
cargo bench -p nitrite_bench -- --save-baseline new-version

# Compare them using critcmp (install with: cargo install critcmp)
critcmp old-version new-version
```

## Benchmark Configuration

Default document counts tested: `100`, `1,000`, `10,000`

Each benchmark runs with:
- **In-memory store**: Fast, no persistence overhead
- **Fjall store**: Persistent storage with disk I/O

## Continuous Integration

For CI environments, use reduced iterations:

```bash
# Quick CI benchmark (faster, less statistical precision)
cargo bench -p nitrite_bench -- --warm-up-time 1 --measurement-time 2
```

## Directory Structure

```
nitrite-bench/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs            # Module declarations
│   ├── config.rs         # Benchmark configuration
│   ├── data_gen.rs       # Document generators
│   └── stores.rs         # Store factory functions
└── benches/
    ├── crud_bench.rs       # CRUD operation benchmarks
    ├── index_bench.rs      # Indexing benchmarks
    ├── spatial_bench.rs    # Spatial query benchmarks
    ├── fts_bench.rs        # Full-text search benchmarks
    ├── concurrency_bench.rs # Concurrent operations
    ├── transaction_bench.rs # Transaction benchmarks
    └── comparison_bench.rs  # Database comparison (optional)
```

## Output Location

- **HTML Reports**: `target/criterion/report/`
- **Raw Data**: `target/criterion/*/`
- **Baselines**: `target/criterion/*/base/` and `target/criterion/*/new/`
