# meta-update Profiling Guide

## Overview

The meta-update tool includes built-in profiling capabilities on two levels:

1. **Go Runtime Profiling** - CPU and heap usage via pprof
2. **Couchbase Query Profiling** - Query execution metrics and timing

This two-layer approach helps identify bottlenecks in both the Go application code and database queries.

## CLI Flags

### Runtime Profiling Flags

#### `-cpuprofile <file>`
Write CPU profile to the specified file. CPU profiles measure where the program spends its time in Go code.

**Default**: (empty, disabled)

**Example**:
```bash
./meta-update -c ~/credentials -s ./settings.json -a ceiling -cpuprofile cpu.pprof
```

#### `-memprofile <file>`
Write heap memory profile to the specified file. Memory profiles show memory allocations and can reveal leaks.

**Default**: (empty, disabled)

**Example**:
```bash
./meta-update -c ~/credentials -s ./settings.json -a ceiling -memprofile mem.pprof
```

### Query Profiling Flags

#### `-query-metrics` (boolean)
Enable collection of Couchbase query metrics (elapsed time, execution time, result count, warnings).

**Default**: `true`

**Example**:
```bash
./meta-update -query-metrics=false   # Disable metrics collection
```

#### `-query-profile <mode>`
Couchbase query profiling mode. Controls how detailed query execution information is captured.

**Options**:
- `off` (or empty) - No query profiling
- `phases` - Captures query phases (parse, plan, execute, etc.)
- `timings` - Captures detailed timing information for each phase

**Default**: `off`

**Example**:
```bash
./meta-update -query-profile=phases   # Capture phase-level profiling
./meta-update -query-profile=timings  # Capture detailed timings
```

#### `-query-slow-ms <milliseconds>`
Log detailed query metrics only for queries exceeding this elapsed time threshold. Use `0` to log all queries.

**Default**: `500`

**Example**:
```bash
./meta-update -query-slow-ms=200    # Log queries slower than 200ms
./meta-update -query-slow-ms=0      # Log all queries
```

#### `-query-summary-top <count>`
Number of slowest query templates to include in the end-of-run summary report. Use `0` to show all.

**Default**: `10`

**Example**:
```bash
./meta-update -query-summary-top=20  # Show top 20 slowest query types
./meta-update -query-summary-top=0   # Show all query types
```

## Usage Scenarios

### Scenario 1: Quick Performance Overview (Low Overhead)

Identify slow queries without detailed profiling:

```bash
./meta-update \
  -c ~/credentials \
  -s ./settings.json \
  -a ceiling \
  -query-metrics=true \
  -query-profile=off \
  -query-slow-ms=200 \
  -query-summary-top=10
```

**What you'll see**:
- Log entries for queries exceeding 200ms
- End-of-run summary of top 10 slowest query templates
- Low overhead, suitable for production runs

### Scenario 2: Deep Query Analysis

Capture full query phase timings for all queries:

```bash
./meta-update \
  -c ~/credentials \
  -s ./settings.json \
  -a ceiling \
  -query-metrics=true \
  -query-profile=timings \
  -query-slow-ms=0 \
  -query-summary-top=5
```

**What you'll see**:
- Detailed timing profile for every query in JSON
- Every query phase breakdown (parse, plan, execute, etc.)
- End-of-run summary of top 5 slowest query types

### Scenario 3: Go Code Profiling

Analyze CPU usage in Go code:

```bash
./meta-update \
  -c ~/credentials \
  -s ./settings.json \
  -a ceiling \
  -cpuprofile cpu.pprof
```

Then visualize:
```bash
go tool pprof -http=:8080 ./meta-update cpu.pprof
```

This opens an interactive web UI showing where CPU time is spent.

### Scenario 4: Memory Usage Analysis

Analyze heap allocations:

```bash
./meta-update \
  -c ~/credentials \
  -s ./settings.json \
  -a ceiling \
  -memprofile mem.pprof
```

Then visualize:
```bash
go tool pprof -http=:8080 ./meta-update mem.pprof
```

### Scenario 5: Full Profiling (Comprehensive)

Capture everything for detailed analysis:

```bash
./meta-update \
  -c ~/credentials \
  -s ./settings.json \
  -a ceiling \
  -cpuprofile cpu.pprof \
  -memprofile mem.pprof \
  -query-metrics=true \
  -query-profile=timings \
  -query-slow-ms=0 \
  -query-summary-top=0
```

## Output Interpretation

### Query Log Output

For each slow query, you'll see log lines like:

```
2026/04/29 14:35:22 query profile [queryWithSQLStringSA] elapsed=1.234s execution=1.100s count=15234 warnings=0 status=success
2026/04/29 14:35:22 query text [queryWithSQLStringSA]: SELECT COUNT(*) FROM `vxdata`._default.METAR WHERE...
2026/04/29 14:35:22 query profile details [queryWithSQLStringSA]: {"#operator":"Sequence",...}
```

**Interpret**:
- `elapsed` - Total time from client perspective (includes network)
- `execution` - Time spent executing on the database
- `count` - Number of results returned
- `warnings` - SQL warnings from the database
- `status` - Query status (success, timeout, etc.)

### Query Summary Report

At the end of execution, you'll see:

```
2026/04/29 14:35:45 query summary: 12 distinct query templates (10 shown)
2026/04/29 14:35:45 query summary #1 [queryWithSQLStringMAP] count=450 total_elapsed=2m30s avg_elapsed=333.3ms max_elapsed=1.234s avg_execution=300ms
2026/04/29 14:35:45 query summary sql #1: SELECT ... FROM `vxdata`._default.METAR WHERE docType = 'CTC'...
2026/04/29 14:35:45 query summary #2 [queryWithSQLStringSA] count=380 total_elapsed=1m45s avg_elapsed=275ms max_elapsed=890ms avg_execution=250ms
2026/04/29 14:35:45 query summary sql #2: SELECT DISTINCT model FROM `vxdata`._default.METAR WHERE...
```

**Interpret**:
- `count` - How many times this query template ran
- `total_elapsed` - Cumulative time spent
- `avg_elapsed` - Average per execution
- `max_elapsed` - Single slowest execution
- `avg_execution` - Average database-only time (excludes network)

### pprof CPU Profile Analysis

After generating `cpu.pprof`:

```bash
go tool pprof -http=:8080 ./meta-update cpu.pprof
```

The web UI shows:
- **Flame Graph** - Visual hierarchy of CPU time usage
- **Table** - Sorted by cumulative time
- **Source** - Annotated source code with CPU samples

Look for:
- Unexpectedly high CPU in utility functions (JSON marshaling, string operations)
- Query helper functions if they're doing excessive work
- Synchronization bottlenecks (mutexes)

### pprof Heap Profile Analysis

After generating `mem.pprof`:

```bash
go tool pprof -http=:8080 ./meta-update mem.pprof
```

Look for:
- Large allocations in slice growth (append operations)
- JSON unmarshaling creating many objects
- Query result buffering

## Recommended Workflow

1. **Initial Run** (Scenario 1)
   - Quick 200ms threshold to identify problem queries
   - Generates summary for prioritization

2. **Deep Dive** (Scenario 2)
   - Focus on slowest queries from Step 1
   - Use `timings` profile to see phase breakdown
   - Consider database index improvements

3. **Code Analysis** (Scenario 3)
   - If queries are fast but overall time is high
   - Use CPU profile to find Go-side bottlenecks

4. **Optimization Loop**
   - Make targeted changes (indexes, SQL, code)
   - Re-run with same flags to measure improvement

## Performance Notes

- **With `-query-slow-ms=500`**: Minimal overhead, suitable for production
- **With `-query-slow-ms=0 -query-profile=timings`**: Moderate overhead (5-10% slower), for troubleshooting
- **With `-cpuprofile` and `-memprofile`**: Can add 10-20% overhead due to profiling instrumentation
- **Memory impact**: Query profiling keeps ~100 distinct query templates in memory; safe for typical workloads

## Example: Full Investigation

```bash
# Run 1: Quick scan (2 minutes)
./meta-update -c ~/credentials -s ./settings.json -a ceiling \
  -query-slow-ms=200 2>&1 | tee run1.log

# Review run1.log for slow queries
# If getDistinctDataKeys is slow, focus on that

# Run 2: Deep dive into slow queries (5 minutes)
./meta-update -c ~/credentials -s ./settings.json -a ceiling \
  -query-profile=timings -query-slow-ms=0 \
  -query-summary-top=0 2>&1 | tee run2.log

# Extract timing data for slow query templates
grep "query summary #1" run2.log
grep "query profile details" run2.log | head -5

# Run 3: CPU profiling to check Go overhead
./meta-update -c ~/credentials -s ./settings.json -a ceiling \
  -cpuprofile cpu.pprof -query-slow-ms=500

# Analyze with pprof
go tool pprof -http=:8080 ./meta-update cpu.pprof
```

## Troubleshooting

**"query profiling configured" log is missing**
- Rebuild: `go build .` in meta_update_middleware

**No query metrics showing**
- Check `-query-metrics=true` is set (it's the default)
- Verify `-query-slow-ms` threshold; queries faster than threshold are not logged

**pprof shows "no samples"**
- The program ran too quickly or the CPU threshold was too high
- Run a larger job or lower profiling threshold

**Memory profile is empty**
- Run a job that allocates significant memory
- Memory profiles capture allocations after `runtime.GC()`

