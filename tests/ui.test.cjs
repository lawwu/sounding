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
  resolve('/api/search?q=&offset=0', {items: [], next_offset: null});
  return {element, context, pending, resolve, run};
}
const table = fqn => ({fqn, type: 'TABLE', columns: [], num_rows: 1, num_bytes: 8, full_scan_usd: 0.00006});
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
    h.resolve(action === 'prev' ? '/api/preview' : '/api/column-costs', action === 'prev' ? [{secret: 'A rows'}] : table('p.d.A'));
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
