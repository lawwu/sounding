"""Offline public-read and metadata-write comparison against v0.7.0.
Takes a benchmark copy (not the user's catalog) and a NEW output directory.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import duckdb
from sounding import catalog

source, root = Path(sys.argv[1]), Path(sys.argv[2])
root.mkdir(parents=True, exist_ok=False)
old_file = root/'old_catalog.py'
old_file.write_bytes(subprocess.check_output(['git','show','v0.7.0:src/sounding/catalog.py']))
spec=importlib.util.spec_from_file_location('sounding._old_catalog',old_file)
old=importlib.util.module_from_spec(spec); spec.loader.exec_module(old)
results={}
for name,module in [('v0.7.0',old),('shared_columns',catalog)]:
    migrated=root/(name+'.duckdb'); shutil.copy2(source,migrated)
    module.CATALOG=migrated
    start=time.perf_counter()
    with module.connect() as con:
        results[name]={'migration_seconds':time.perf_counter()-start}
        fqns=[r[0] for r in con.execute(module.LATEST+' SELECT fqn FROM current_tables ORDER BY fqn').fetchall()]
        descriptions=[module.describe(con,fqn) for fqn in fqns]
        searches=[]
        start=time.perf_counter()
        for _ in range(5):
            searches=[module.search(con,q) for q in ('','id','name','no_such_column_123')]
        results[name]['public_search_seconds']=(time.perf_counter()-start)/20
        payload=json.dumps({'tables':descriptions,'search':searches,'browse':module.browse(con),'stats':module.stats(con)},sort_keys=True)
        results[name]['public_output_sha256']=hashlib.sha256(payload.encode()).hexdigest()
        cur=con.execute(module.LATEST+' SELECT * FROM current_tables ORDER BY dataset,fqn')
        names=[d[0] for d in cur.description]
        tables=[dict(zip(names,row)) for row in cur.fetchall()]
        for row in tables:
            for key in module.JSON_FIELDS:
                if row.get(key) is not None: row[key]=json.loads(row[key])
            if row.get('schema_hash'):
                row['resource']['schema']=json.loads(con.execute('SELECT schema FROM schemas WHERE schema_hash=?',[row['schema_hash']]).fetchone()[0])
        cur=con.execute(module.LATEST+' SELECT c.snapshot_id,c.fqn,c.path,c.type,c.mode,c.description,c.scan_bytes,c.policy_tags FROM columns c JOIN current_tables t USING(snapshot_id,fqn) ORDER BY c.fqn,c.rowid')
        names=[d[0] for d in cur.description]
        columns=[dict(zip(names,row)) for row in cur.fetchall()]
    module.CATALOG=root/(name+'-replay.duckdb')
    with module.connect(): pass
    start=time.perf_counter()
    for dataset in sorted(set(t['dataset'] for t in tables)):
        selected=[t for t in tables if t['dataset']==dataset]
        names={t['fqn'] for t in selected}
        module.save_snapshot(dataset,selected,[c for c in columns if c['fqn'] in names])
    results[name]['metadata_write_seconds']=time.perf_counter()-start
    results[name]['tables_replayed']=len(tables)
    results[name]['columns_replayed']=len(columns)
    with module.connect() as con:
        assert con.execute('SELECT count(*) FROM columns').fetchone()[0] == len(columns)
        results[name]['stored_definitions']=con.execute('SELECT count(*) FROM '+('columns' if name=='v0.7.0' else 'column_definitions')).fetchone()[0]
assert results['v0.7.0']['public_output_sha256']==results['shared_columns']['public_output_sha256']
(root/'measurements.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
