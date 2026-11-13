const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../src/sounding/ui.js'), 'utf8');

function harness() {
  const elements = new Map(), pending = [];
  function element(selector) {
    if (!elements.has(selector)) elements.set(selector, {
      value: '', content: 'session-token', innerHTML: '', textContent: '', hidden: false,
      disabled: false, dataset: {}, classList: {toggle() {}}, addEventListener() {},
      querySelectorAll: () => [], setAttribute() {},
      focus() {}, insertAdjacentHTML(where, html) { this.innerHTML += html; },
    });
    return elements.get(selector);
  }
  const context = {
    console, setTimeout, clearTimeout,
    document: {querySelector: element, querySelectorAll: () => [], addEventListener() {}, activeElement: {tagName: 'BODY'}},
    history: {replaceState(_a, _b, hash) {context.hash = hash;}}, location: {hash: ''},
    fetch(url, options) { return new Promise(resolve => pending.push({url, options, resolve})); },
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  function resolve(url, body, ok = true) {
    const request = pending.find(p => p.url === url && !p.done);
    assert.ok(request, `missing request ${url}`);
    request.done = true;
    request.resolve({ok, json: async () => body});
    return request;
  }
  const run = code => vm.runInContext(code, context);
  resolve('/api/stats', {datasets: 0, tables: 0, bytes: 0});
  resolve('/api/browse', {projects: []});
  resolve('/api/refresh', {status: 'idle'});
  resolve('/api/search?q=&offset=0', {items: [], next_offset: null});
  return {element, context, pending, resolve, run};
}
const table = fqn => ({fqn, type: 'TABLE', columns: [], num_rows: 1, num_bytes: 8, full_scan_usd: 0.00006});

test('offender ranking switches metrics locally and labels missing totals', async () => {
  const h = harness();
  h.context.report = {jobs:[],query_groups:[
    {representative:{job_id:'scan-heavy'},executions:2,known_billed_bytes:500,known_slot_ms:10,unknown_billed:0,unknown_slots:0},
    {representative:{job_id:'slot-heavy'},executions:8,known_billed_bytes:100,known_slot_ms:900,unknown_billed:1,unknown_slots:0}]};
  h.run('renderCostRanking(report, viewSeq)');
  let html = h.element('#offenders').innerHTML;
  assert.ok(html.indexOf('scan-heavy') < html.indexOf('slot-heavy'));
  assert.match(html, /1 unknown/);
  h.element('#offender-rank').value = 'slots'; h.run('renderCostRanking(report, viewSeq)');
  html = h.element('#offenders').innerHTML;
  assert.ok(html.indexOf('slot-heavy') < html.indexOf('scan-heavy'));
  assert.equal(h.pending.filter(p => p.url === '/api/inspect-job').length, 0);
});

test('cost report queries are fetched on demand, escaped, and reused when reopened', async () => {
  const h = harness();
  h.run('reportPage()');
  h.context.button = h.element('#query-button');
  const load = h.run('showReportQuery({job_id:"j",project:"p",location:"EU"}, 0, button, viewSeq)');
  const req = h.resolve('/api/inspect-job', {sql:'SELECT "<script>"',sql_truncated:true});
  assert.deepEqual(JSON.parse(req.options.body), {job_id:'j',project:'p',location:'EU',include_sql:true});
  await load;
  assert.match(h.element('#report-query-0').innerHTML, /&lt;script&gt;/);
  assert.match(h.element('#report-query-0').innerHTML, /truncated/);
  h.run('showReportQuery({}, 0, button, viewSeq)');
  assert.equal(h.element('#report-query-row-0').hidden, true);
  h.run('showReportQuery({}, 0, button, viewSeq)');
  assert.equal(h.element('#report-query-row-0').hidden, false);
  assert.equal(h.pending.filter(p => p.url === '/api/inspect-job').length, 1);
});

test('late cost report SQL cannot overwrite another page', async () => {
  const h = harness(); h.run('reportPage()'); h.context.button = h.element('#query-button');
  const load = h.run('showReportQuery({job_id:"j",project:"p",location:"US"}, 0, button, viewSeq)');
  h.run('reportPage()'); h.element('#report-query-0').innerHTML = 'new report';
  h.resolve('/api/inspect-job', {sql:'old query'}); await load;
  assert.equal(h.element('#report-query-0').innerHTML, 'new report');
});
async function show(h, fqn) {
  const promise = h.run(`open(${JSON.stringify(fqn)})`);
  h.resolve('/api/table?fqn=' + encodeURIComponent(fqn), table(fqn));
  await promise;
}

test('out-of-order table responses cannot replace the latest selection', async () => {
  const h = harness();
  const a = h.run('open("p.d.A")'), b = h.run('open("p.d.B")');
  h.resolve('/api/table?fqn=p.d.B', table('p.d.B')); await b;
  h.resolve('/api/table?fqn=p.d.A', table('p.d.A')); await a;
  assert.equal(h.context.hash, '#p.d.B');
  assert.match(h.element('#detail').innerHTML, /p\.d\.B/);
  assert.doesNotMatch(h.element('#detail').innerHTML, /p\.d\.A/);
});

test('late preview and column-cost responses cannot modify another table', async () => {
  for (const action of ['prev', 'costs']) {
    const h = harness(); await show(h, 'p.d.A');
    const button = h.element('#' + action);
    const promise = button.onclick({target: button});
    await show(h, 'p.d.B');
    const before = h.element('#detail').innerHTML;
    h.element('#prevout').innerHTML = 'B preview';
    h.resolve(action === 'prev' ? '/api/refresh-sample' : '/api/column-costs', action === 'prev' ? {preview_rows:[{secret: 'A rows'}]} : table('p.d.A'));
    await promise;
    assert.equal(h.element('#detail').innerHTML, before);
    assert.equal(h.element('#prevout').innerHTML, 'B preview');
  }
});

test('out-of-order estimates and edits ignore stale responses', async () => {
  const h = harness(); await show(h, 'p.d.A');
  h.element('#sql').value = 'SELECT 1';
  const old = h.run('estimate()');
  h.element('#sql').value = 'SELECT 2';
  const fresh = h.run('estimate()');
  const response = n => ({bytes:n, estimate_status:'estimated', usd_on_demand_range:{min:1,max:1},
    referenced_tables:[], referenced_tables_complete:true, warnings:[], price_basis:'US list price'});
  const requests = h.pending.filter(p => p.url === '/api/estimate');
  requests[1].resolve({ok:true,json:async()=>response(222)}); await fresh;
  requests[0].resolve({ok:true,json:async()=>response(111)}); await old;
  assert.match(h.element('#estout').innerHTML, /222 B/);
  assert.doesNotMatch(h.element('#estout').innerHTML, /111 B/);
  const editing = h.run('estimate()');
  h.element('#sql').oninput();
  const latest = h.pending.filter(p => p.url === '/api/estimate').at(-1);
  latest.resolve({ok:true,json:async()=>response(333)}); await editing;
  assert.doesNotMatch(h.element('#estout').innerHTML, /333 B/);
});

test('unknown cost is displayed as unknown, never zero or no tables', async () => {
  const h = harness(); await show(h, 'p.d.A');
  h.element('#sql').value = 'EXECUTE IMMEDIATE "SELECT 1"';
  const p = h.run('estimate()');
  h.resolve('/api/estimate',{bytes:null,estimate_status:'unknown',usd_on_demand_range:null,
    referenced_tables:[],referenced_tables_complete:false,warnings:['Cost unknown'],price_basis:'US scan price'});
  await p;
  assert.match(h.element('#estout').innerHTML,/Unknown scan size/);
  assert.match(h.element('#estout').innerHTML,/may be incomplete/);
  assert.doesNotMatch(h.element('#estout').innerHTML,/\$0|no tables/);
});

test('untrusted text and highlighting remain escaped without splitting entities', async () => {
  const h = harness();
  assert.equal(h.run('hl("a&b", "amp")'), 'a&amp;b');
  assert.equal(h.run('hl("<img src=x onerror=alert(1)>", "img")'), '&lt;<mark>img</mark> src=x onerror=alert(1)&gt;');
  assert.equal(h.run('hl("a[b", "[")'), 'a<mark>[</mark>b');
  assert.equal(h.run('hl("a&b", "&")'), 'a<mark>&amp;</mark>b');
  const payload = '</textarea><img src=x onerror=alert(1)>';
  const value = table('p.d.A');
  value.description = payload;
  value.columns = [{path:payload,type:payload,mode:payload,description:payload}];
  h.context.payload = value;
  h.run('renderTable(payload)');
  assert.doesNotMatch(h.element('#detail').innerHTML, /<img/);
  assert.match(h.element('#detail').innerHTML, /&lt;img/);
});

test('pagination keeps result limits and API calls carry the session token', async () => {
  const h = harness();
  const p = h.run('search()');
  h.resolve('/api/search?q=&offset=0', {items:[{table:'p.d.t',matched_columns:['hits'],matched_column_count:320}],next_offset:100});
  await p;
  assert.match(h.element('#list').innerHTML,/\+316/);
  assert.equal(h.element('#more').hidden,false);
  const more = h.run('search(100)');
  h.resolve('/api/search?q=&offset=100',{items:[],next_offset:null}); await more;
  assert.equal(h.element('#more').hidden,true);
  for(const r of h.pending) assert.equal(r.options.headers['X-Sounding-Token'],'session-token');
});

test('dataset selection scopes search and stale dataset responses cannot replace a table', async () => {
  const h = harness();
  const dataset = h.run('openDataset("p.d")');
  h.resolve('/api/search?q=&offset=0&project=p&dataset=p.d', {items: [], next_offset: null});
  await show(h, 'p.d.A');
  h.resolve('/api/dataset?dataset=p.d', {dataset: 'p.d', tables: 0, routines: []});
  await dataset;
  assert.match(h.element('#detail').innerHTML, /p\.d\.A/);
  assert.equal(h.element('#scope').textContent, 'p.d');
  assert.ok(h.pending.every(p => !p.options.method), 'browsing only issues local GETs');
});

test('switching project discards old results and retains scope when paging', async () => {
  const h = harness();
  h.run('overview("p")');
  h.run('overview("p2")');
  const row = name => ({table:name,matched_columns:[],matched_column_count:0});
  h.resolve('/api/search?q=&offset=0&project=p2', {items:[row('p2.d.new')],next_offset:100});
  await new Promise(resolve => setImmediate(resolve));
  h.resolve('/api/search?q=&offset=0&project=p', {items:[row('p.d.old')],next_offset:null});
  await new Promise(resolve => setImmediate(resolve));
  assert.match(h.element('#list').innerHTML, /new/);
  assert.doesNotMatch(h.element('#list').innerHTML, /old/);
  const more = h.run('search(100)');
  h.resolve('/api/search?q=&offset=100&project=p2', {items:[],next_offset:null});
  await more;
  assert.equal(h.element('#more').hidden, true);
});

test('hierarchy and dataset descriptions are escaped and empty datasets can be opened', async () => {
  const h = harness();
  await new Promise(resolve => setImmediate(resolve));
  h.context.tree = [{project: 'p', tables:0, routines:0, datasets:[{dataset:'p.empty',dataset_id:'<img src=x>',tables:0,routines:0,description:'<script>bad</script>'}]}];
  h.run('hierarchy = tree; overview("p")');
  assert.doesNotMatch(h.element('#tree').innerHTML, /<img/);
  assert.match(h.element('#detail').innerHTML, /&lt;script&gt;/);
  const opening = h.run('openDataset("p.empty")');
  h.resolve('/api/dataset?dataset=p.empty', {dataset:'p.empty',tables:0,routines:[],description:'<img src=x>'});
  await opening;
  assert.match(h.element('#detail').innerHTML, /no tables in its latest saved snapshot/);
  assert.doesNotMatch(h.element('#detail').innerHTML, /<img/);
  assert.equal(h.context.hash, '#dataset/p.empty');
});

test('menu pages are separate and a late report cannot replace documentation', async () => {
  const h = harness();
  const report = h.run('costReport()');
  const docs = h.run('docsPage()');
  h.resolve('/api/docs', {html:'<h2>CLI and MCP</h2>'}); await docs;
  h.resolve('/api/cost-report', {project:'p', jobs:[]}); await report;
  assert.equal(h.element('#detail').innerHTML, '<h2>CLI and MCP</h2>');
  assert.equal(h.element('#results').hidden, true);
  assert.equal(h.element('#q').hidden, true);
  h.run('overview()');
  assert.equal(h.element('#results').hidden, false);
});

test('schema roles and table history remain tied to the current table', async () => {
  const h = harness(); await show(h, 'p.d.A');
  h.context.metadata = {...table('p.d.A'), partition:{kind:'DAY'}, columns:[{path:'day',type:'DATE',partition_column:true,clustering_order:1,primary_key:true}]};
  h.run('renderTable(metadata)');
  assert.match(h.element('#detail').innerHTML, /partition · DAY/);
  assert.match(h.element('#detail').innerHTML, /cluster 1/);
  assert.match(h.element('#detail').innerHTML, /value="30"/);
  h.element('#query-days').value = '30'; h.element('#query-scope').value = 'user'; h.element('#query-rank').value = 'billed_bytes';
  const history = h.element('#load-queries').onclick();
  const request = h.pending.find(p => p.url === '/api/table-queries');
  assert.equal(JSON.parse(request.options.body).days, 30);
  await show(h, 'p.d.B');
  h.element('#queries-out').innerHTML = 'B history';
  h.resolve('/api/table-queries', {jobs:[]}); await history;
  assert.equal(h.element('#queries-out').innerHTML, 'B history');
});

test('refresh status does not replace another tab and escapes failures', async () => {
  const h = harness(); await new Promise(resolve => setImmediate(resolve));
  h.context.refresh = {id:'j',status:'partial',phase:'finished',progress:{datasets_done:1,datasets_total:1,done:1,total:2},datasets:[{dataset:'p.d',status:'failed',done:1,total:2,message:'<img src=x>'}],projects:[],warnings:[]};
  h.run('refreshState = refresh; refreshPage()');
  assert.match(h.element('#refresh-progress').innerHTML, /&lt;img/);
  assert.doesNotMatch(h.element('#refresh-progress').innerHTML, /<img/);
  h.run('reportPage()');
  const content = h.element('#detail').innerHTML;
  h.run('updateRefreshProgress()');
  assert.equal(h.element('#detail').innerHTML, content);
  assert.equal(h.element('#refresh-banner').hidden, false);
});

test('an older progress poll cannot hide a newly started refresh', async () => {
  const h = harness(); await new Promise(resolve => setImmediate(resolve));
  const oldPoll = h.run('pollRefresh()');
  h.run('refreshPage(null, "p.d")');
  const starting = h.run('startRefresh()');
  const post = h.pending.find(p => p.url === '/api/refresh' && p.options.method === 'POST');
  post.done = true;
  post.resolve({ok:true,json:async()=>({id:'new',status:'running',phase:'harvesting',progress:{datasets_done:0,datasets_total:1,done:0,total:1},projects:[],datasets:[]})});
  await starting;
  h.resolve('/api/refresh', {status:'idle'}); await oldPoll;
  assert.equal(h.run('refreshState.id'), 'new');
  assert.equal(h.element('#start-refresh').disabled, true);
});

test('opening a project starts metadata import and switches to its catalog', async () => {
  const h = harness(); await new Promise(resolve => setImmediate(resolve));
  h.element('#project-id').value = 'new-project';
  const opening = h.run('openProject({preventDefault(){}})');
  const post = h.pending.find(p => p.url === '/api/refresh' && p.options.method === 'POST');
  assert.deepEqual(JSON.parse(post.options.body), {projects:['new-project'], datasets:[]});
  post.done = true; post.resolve({ok:true,json:async()=>({id:'j',status:'running',phase:'discovering',projects:[],datasets:[]})});
  await opening;
  assert.equal(h.context.hash, '#project/new-project');
  assert.match(h.element('#detail').innerHTML, /new-project/);
  assert.equal(h.element('#open-project').disabled, true);
  h.resolve('/api/recommendations?project=new-project', {scopes:[]});
  assert.ok(h.pending.some(p => p.url.includes('project=new-project') && p.url.startsWith('/api/search')));
});

test('new metadata distinguishes denied from empty and escapes data', async () => {
  const h = harness();
  h.context.record = {...table('p.d.t'), row_access_policies_status:'denied', models_status:'denied',
    preview_status:'harvested', preview_sampled_at:'2026-11-13', preview_rows:[{x:'<img src=x>'}],
    recommendations:{scopes:[{kind:'recommendations',location:'us',status:'disabled',message:'<script>x</script>',items:[],stale:true}]}};
  h.run('renderTable(record)');
  assert.match(h.element('#detail').innerHTML, /Access denied/);
  assert.doesNotMatch(h.element('#detail').innerHTML, /No row-access policies|<img|<script>/);
  assert.match(h.element('#detail').innerHTML, /&lt;img/);
  assert.match(h.element('#detail').innerHTML, /stale \/ incomplete/);
});

test('late model responses cannot replace a table', async () => {
  const h = harness();
  const opening = h.run('openModel("p.d.m")');
  await show(h, 'p.d.t');
  h.resolve('/api/model?fqn=p.d.m', {fqn:'p.d.m',metadata_status:'harvested'});
  await opening;
  assert.match(h.element('#detail').innerHTML, /p\.d\.t/);
  assert.doesNotMatch(h.element('#detail').innerHTML, /Input features/);
});

test('inventory hydration is explicit and a late result cannot replace another table', async () => {
  const h = harness(); await show(h, 'p.d.A');
  h.context.inventory = {...table('p.d.A'), metadata_level:'inventory'};
  h.run('renderTable(inventory)');
  assert.match(h.element('#detail').innerHTML, /Inventory only/);
  assert.doesNotMatch(h.element('#detail').innerHTML, /id="costs"/);
  assert.equal(h.pending.filter(p => p.url === '/api/load-metadata').length, 0);
  const loading = h.element('#load-metadata').onclick({target:h.element('#load-metadata')});
  await show(h, 'p.d.B');
  const html = h.element('#detail').innerHTML;
  h.resolve('/api/load-metadata', {...table('p.d.A'), metadata_level:'full'}); await loading;
  assert.equal(h.element('#detail').innerHTML, html);
});

test('refresh forwards inventory mode, filters, hidden selection and concurrency', async () => {
  const h = harness(); await new Promise(resolve => setImmediate(resolve));
  h.run('refreshPage()');
  h.element('#refresh-projects').value = 'p, q';
  h.element('#refresh-datasets').value = '';
  h.element('#refresh-mode').value = 'inventory';
  h.element('#refresh-include').value = '*prod*';
  h.element('#refresh-exclude').value = 'stage*, *_scratch';
  h.element('#refresh-hidden').checked = true;
  h.element('#refresh-concurrency').value = '2';
  const starting = h.run('startRefresh()');
  const request = h.pending.find(p => p.url === '/api/refresh' && p.options.method === 'POST');
  assert.deepEqual(JSON.parse(request.options.body), {projects:['p','q'], datasets:[], inventory_only:true,
    include_datasets:['*prod*'], exclude_datasets:['stage*','*_scratch'], include_hidden:true, project_concurrency:2, resume:false});
  h.resolve('/api/refresh', {status:'complete', datasets:[], projects:[]}); await starting;
  h.resolve('/api/refresh', {status:'idle'});
});

test('resume uses the saved import scope rather than current selections', async () => {
  const h = harness(); await new Promise(resolve => setImmediate(resolve));
  h.run('refreshState = {id:"saved",status:"interrupted",requested_projects:["p"],requested_datasets:[],inventory_only:true,filters:{exclude_datasets:["stage*"]},project_concurrency:2}; refreshPage()');
  const starting = h.element('#resume-import').onclick();
  const request = h.pending.find(p => p.url === '/api/refresh' && p.options.method === 'POST');
  const body = JSON.parse(request.options.body);
  assert.equal(body.resume, true); assert.deepEqual(body.projects, ['p']);
  assert.equal(body.inventory_only, true); assert.deepEqual(body.exclude_datasets, ['stage*']);
  h.resolve('/api/refresh', {status:'complete',datasets:[],projects:[]}); await starting;
  h.resolve('/api/refresh', {status:'idle'});
});
