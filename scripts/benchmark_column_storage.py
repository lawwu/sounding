"""Compare legacy, smaller-key, and shared-column layouts on copies, never the source.
Run: uv run python scripts/benchmark_column_storage.py CATALOG OUTPUT_DIRECTORY
"""
import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
from filelock import FileLock
from sounding import column_storage

source, root = Path(sys.argv[1]).expanduser(), Path(sys.argv[2])
root.mkdir(parents=True, exist_ok=False)
with FileLock(str(source)+'.lock'):
    if Path(str(source)+'.wal').exists():
        raise RuntimeError('Close the catalog writer and checkpoint before benchmarking.')
    shutil.copy2(source, root/'original.duckdb')
results = {}
for variant in ('original', 'integer_keys', 'shared_columns'):
    path=root/(variant+'.duckdb')
    if variant != 'original':
        shutil.copy2(root/'original.duckdb',path)
    with duckdb.connect(str(path)) as con:
        start=time.perf_counter()
        if variant == 'integer_keys':
            con.execute(column_storage.DDL)
            con.execute('''CREATE TABLE compact_columns AS SELECT t.table_snapshot_id,c.* EXCLUDE(snapshot_id,fqn)
                           FROM columns c JOIN tables t USING(snapshot_id,fqn)''')
            con.execute('ALTER TABLE compact_columns ADD PRIMARY KEY(table_snapshot_id,path)')
            con.execute('DROP TABLE columns')
            con.execute('''CREATE VIEW columns AS SELECT t.snapshot_id,t.fqn,c.* EXCLUDE(table_snapshot_id)
                           FROM compact_columns c JOIN tables t USING(table_snapshot_id)''')
        elif variant == 'shared_columns':
            con.execute('BEGIN')
            column_storage.migrate(con)
            con.execute('COMMIT')
        elapsed=time.perf_counter()-start
        queries = ["SELECT count(*) FROM columns WHERE contains(lower(path),'id')",
                   "SELECT count(*) FROM columns WHERE contains(lower(path),'name')"]
        start=time.perf_counter()
        for _ in range(10):
            counts=[con.execute(q).fetchone()[0] for q in queries]
        search=(time.perf_counter()-start)/20
        results[variant]={'migration_seconds':elapsed, 'column_rows':con.execute('SELECT count(*) FROM columns').fetchone()[0],
                          'search_seconds':search, 'search_counts':counts}
        target=root/(variant+'-compacted.duckdb')
        con.execute('ATTACH ? AS compacted', [str(target)])
        con.execute(f'COPY FROM DATABASE {con.execute("SELECT current_database()").fetchone()[0]} TO compacted')
        con.execute('DETACH compacted')
    results[variant]['file_bytes']=path.stat().st_size
    results[variant]['compacted_bytes']=target.stat().st_size
(root/'measurements.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
