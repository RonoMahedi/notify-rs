// Drives headless Chrome over the DevTools Protocol to test the admin UI.
import { writeFileSync } from "node:fs";
const OUT = process.argv[2], BASE = "http://localhost:8090", TOKEN = process.env.ADMIN_TOKEN || "dev-admin-token-change-me";
const EMAIL = process.env.UI_EMAIL, PW = process.env.UI_PW;
if (!EMAIL || !PW) { console.error("run via tests/run_ui.sh (sets UI_EMAIL / UI_PW)"); process.exit(2); }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let PASS = 0, FAIL = 0;
const check = (name, ok, detail = "") => { ok ? PASS++ : FAIL++; console.log(`  ${ok ? "PASS" : "FAIL"} ${name}${detail ? " (" + detail + ")" : ""}`); };

const target = await (await fetch("http://127.0.0.1:9333/json/new?about:blank", { method: "PUT" })).json();
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((r) => (ws.onopen = r));
let seq = 0; const pending = new Map(); const errors = [];
ws.onmessage = (m) => {
  const d = JSON.parse(m.data);
  if (d.id && pending.has(d.id)) { pending.get(d.id)(d); pending.delete(d.id); }
  if (d.method === "Runtime.exceptionThrown") errors.push(d.params.exceptionDetails.exception?.description || d.params.exceptionDetails.text);
  if (d.method === "Runtime.consoleAPICalled" && d.params.type === "error") errors.push(d.params.args.map((a) => a.value ?? a.description).join(" "));
  if (d.method === "Log.entryAdded" && d.params.entry.level === "error" && !/favicon/.test(d.params.entry.url || "")) errors.push(d.params.entry.text + " " + (d.params.entry.url || ""));
};
const cdp = (method, params = {}) => new Promise((r) => { const id = ++seq; pending.set(id, r); ws.send(JSON.stringify({ id, method, params })); });
const js = async (expr) => { const r = await cdp("Runtime.evaluate", { expression: "{" + expr + "}", awaitPromise: true, returnByValue: true }); return r.result?.result?.value; };
const shot = async (name) => { const r = await cdp("Page.captureScreenshot", { format: "png", captureBeyondViewport: true }); writeFileSync(`${OUT}/${name}.png`, Buffer.from(r.result.data, "base64")); };
const until = async (expr, ms = 8000) => { const t = Date.now(); while (Date.now() - t < ms) { if (await js(expr)) return true; await sleep(200); } return false; };
const api = (path, opts = {}) => fetch(BASE + "/v1" + path, { ...opts, headers: { Authorization: "Bearer " + TOKEN, "content-type": "application/json" } }).then((r) => r.json());

await cdp("Page.enable"); await cdp("Runtime.enable"); await cdp("Log.enable");
await cdp("Emulation.setDeviceMetricsOverride", { width: 1280, height: 900, deviceScaleFactor: 1, mobile: false });
await cdp("Page.navigate", { url: BASE + "/admin" }); await sleep(1200);

console.log("== login");
check("sign-in form has email + password", await js("!!document.querySelector('#em') && document.querySelector('#pw').type === 'password'"));
await shot("1-login");
const signIn = async (email, pw) => js(`document.querySelector('#em').value=${JSON.stringify(email)}; document.querySelector('#pw').value=${JSON.stringify(pw)}; document.querySelector('#login').requestSubmit()`);
await signIn(EMAIL, "definitely-wrong-pw");
check("wrong password says 'Wrong email or password'", await until("[...document.querySelectorAll('.toast')].some(t => /Wrong email or password/.test(t.textContent))"));
check("password box cleared after a failure", await js("document.querySelector('#pw').value === ''"));
await signIn(EMAIL, PW);
check("signs in; admin sees 9 tabs incl. Templates, People, Deliveries, System, Users", await until("document.querySelectorAll('nav button').length === 9 && /Templates.*People.*Deliveries.*System.*Users/.test(document.querySelector('nav').textContent)"));
check("header shows who is signed in", await js(`document.querySelector('#acct').textContent.includes(${JSON.stringify(EMAIL)})`));
check("session cookie is not readable by JavaScript (HttpOnly)", await js("!document.cookie.includes('notify_session')"));
await cdp("Page.reload"); await sleep(1500);
check("still signed in after reload", await until("document.querySelectorAll('nav button').length === 9"));
errors.length = 0; // the 401s from the deliberate wrong-password attempt are expected

console.log("== overview");
check("stat cards render a contacts number", await until("/[0-9]/.test(document.querySelector('#stats .value')?.textContent || '')"));
const stats = await api("/stats");
check("contacts card matches API", (await js("document.querySelector('#stats .value').textContent")) === stats.contacts_estimate.toLocaleString(), `${stats.contacts_estimate}`);
check("sync card says healthy", await until("/healthy/.test(document.querySelector('#sync')?.textContent || '')"));
await shot("2-overview");

console.log("== campaigns");
await js("document.querySelector('nav button[data-v=campaigns]').click()");
check("campaign list shows Winter sale", await until("/Winter sale/.test(document.querySelector('#clist')?.textContent || '')"));
check("completed campaigns show 100%", await js("/100%/.test(document.querySelector('#clist').textContent)"));
const segs = await api("/segments"); const seg = segs.find((s) => s.name === "Active 30d, verified");
await until("document.querySelector('#segsel').options.length > 1");
await js(`const s=document.querySelector('#segsel'); s.value='${seg.id}'; s.dispatchEvent(new Event('change'))`);
// A cold count over 1M contacts can exceed the 15 s limit on a small VM: retry, and say why if it still fails.
let want, last;
for (let i = 0; i < 3 && want === undefined; i++) { last = await api(`/segments/${seg.id}/preview?channel=email`); want = last.count; }
if (want === undefined) { check("cohort preview API answered", false, JSON.stringify(last)); want = -1; }
check("audience count shown for chosen cohort", await until(`document.querySelector('#aud').textContent.includes('${want.toLocaleString()}')`), `${want.toLocaleString()} people`);
await shot("3-campaigns");
// Create a draft from the form, then cancel it from the list (two-click confirm).
const DRAFT = "UI draft " + Date.now();
await until("[...document.querySelectorAll('#tplsel option')].some(o => o.value === 'promo_weekly')");
check("campaign form offers this channel's templates", await js("[...document.querySelectorAll('#tplsel option')].map(o => o.value).includes('promo_weekly')"));
await js(`const f=document.querySelector('#cf'); f.name.value='${DRAFT}'; f.template.value='promo_weekly'; f.when.value='draft'; f.requestSubmit()`);
const rowSel = `[...document.querySelectorAll('#clist tr')].find(r => r.textContent.includes('${DRAFT}'))`;
const cancelSel = `${rowSel}?.querySelector('button[data-a=cancel]')`;
check("form creates a draft", await until(`${rowSel}?.querySelector('.pill')?.textContent === 'draft' && !!(${cancelSel})`));
await js(`${cancelSel}.click()`);
check("cancel asks to confirm first", await js(`/confirm/.test(${cancelSel}?.textContent || '')`));
await sleep(3300); // let the 3 s list refresh re-render the row
check("confirm state survives the list refresh", await js(`/confirm/.test(${cancelSel}?.textContent || '')`));
await js(`${cancelSel}.click()`);
check("confirmed cancel -> cancelled", await until(`${rowSel}?.querySelector('.pill')?.textContent === 'cancelled'`));

console.log("== cohorts");
await js("document.querySelector('nav button[data-v=cohorts]').click()");
await until("!!document.querySelector('#prev')");
await js("const p=document.querySelector('#pch'); p.value='email'; document.querySelector('#prev').click()");
const vip = (await api("/segments/preview", { method: "POST", body: JSON.stringify({ channel: "email", rule: { all: [{ field: "attributes.plan", op: "in", value: ["gold", "premium"] }] } }) })).count;
check("rule builder preview = API count (plan in gold, premium)", await until(`document.querySelector('#presult').textContent.includes('${vip.toLocaleString()}')`), `${vip.toLocaleString()}`);
check("preview shows sample rows", await js("document.querySelectorAll('#presult table tr').length > 1"));
check("no broken HTML entities in tables", await js("!/&(quot|amp|lt|gt|#39)/.test(document.querySelector('main').innerText) && !/&qu|&am|&#3/.test(document.querySelector('main').innerText)"));
await js("document.querySelector('#addc').click()");
check("add condition adds a row (within 30d)", await js("document.querySelectorAll('.cond').length === 2 && document.querySelectorAll('.cond')[1].querySelector('.o').value === 'within'"));
await shot("4-cohorts");

console.log("== XSS: hostile contact data is shown as text");
await api("/contacts", { method: "POST", body: JSON.stringify({ contacts: [{ user_id: "xss-test-1", email: "xss@example.com", first_name: "<img src=x onerror=window.__pwned=1>", attributes: { plan: "<script>window.__pwned=2</script>" } }] }) });
await js("document.querySelector('#rjson').value = JSON.stringify({all:[{field:'user_id',op:'eq',value:'xss-test-1'}]}); document.querySelector('#prev').click()");
await until("document.querySelector('#presult').textContent.includes('onerror')");
check("no <img> injected into page", await js("document.querySelectorAll('#presult img').length === 0"));
check("payload displayed as literal text", await js("document.querySelector('#presult').textContent.includes('<img src=x onerror')"));
check("payload never executed", await js("window.__pwned === undefined"));

console.log("== audience");
await js("document.querySelector('nav button[data-v=audience]').click()");
check("imports table lists customers.xlsx", await until("/customers\\.xlsx/.test(document.querySelector('#ilist')?.textContent || '')"));
await js("document.querySelector('#ilist button[data-e]')?.click()");
check("row errors open with row numbers", await until("/19,992/.test(document.querySelector('#ierr')?.textContent || '')"));
await shot("5-audience");

console.log("== templates");
await js("document.querySelector('nav button[data-v=templates]').click()");
check("templates list shows seeded templates", await until("/promo_weekly/.test(document.querySelector('#tlist')?.textContent || '') && /hidden in history/.test(document.querySelector('#tlist').textContent)"));
const TK = "ui_tpl_" + Date.now();
await js(`const f=document.querySelector('#tf'); f.key.value='${TK}'; f.channel.value='sms'; f.channel.dispatchEvent(new Event('change')); f.body.value='আপনার কোড {{code}}। ধন্যবাদ, '; document.querySelector('button[data-tok=first_name]').click(); document.querySelector('#pv-who').value='user12@example.com'; document.querySelector('#pv-who').dispatchEvent(new Event('input'))`);
check("SMS hides the subject field", await js("document.querySelector('#trow').hidden"));
check("live preview fills tokens for a real person", await until("/482913/.test(document.querySelector('#pv-out').textContent) && /As received by user12@example.com/.test(document.querySelector('#pv-out').textContent)"));
check("preview counts Bengali SMS as Unicode parts", await js("/Unicode/.test(document.querySelector('#pv-out').textContent) && /SMS part/.test(document.querySelector('#pv-out').textContent)"));
await js("document.querySelector('#tf').requestSubmit()");
check("template saved and listed", await until(`/${TK}/.test(document.querySelector('#tlist').textContent)`));
await shot("12-templates");
await js(`const r=[...document.querySelectorAll('#tlist tr')].find(r => r.textContent.includes('${TK}')); r.querySelector('[data-del]').click(); r.querySelector('[data-del]').click()`);
check("template deleted (after confirm)", await until(`!/${TK}/.test(document.querySelector('#tlist').textContent)`));

console.log("== people");
await js("document.querySelector('nav button[data-v=people]').click()");
check("people list loads a page of 50", await until("document.querySelectorAll('#plist tr[data-u]').length === 50"));
await js("const f=document.querySelector('#ps'); f.q.value='user7@example.com'; f.requestSubmit()");
check("email search finds exactly user7", await until("document.querySelectorAll('#plist tr[data-u]').length === 1 && /user7@example.com/.test(document.querySelector('#plist').textContent)"));
await js("document.querySelector('#plist tr[data-u]').click()");
check("person detail shows the full FCM token", await until("/fcm-test-token-1/.test(document.querySelector('#pdetail')?.textContent || '')"));
await js("const f=document.querySelector('#ps'); f.q.value='user12@'; f.requestSubmit()");
await until("/user12@example.com/.test(document.querySelector('#plist').textContent)");
await js("document.querySelector('#plist tr[data-u]').click()");
check("person history lists their notifications", await until("/history_check/.test(document.querySelector('#phist')?.textContent || '') && /suppressed/.test(document.querySelector('#phist').textContent)"));
check("history shows the reason for a non-delivery", await js("/user unsubscribed from this channel/.test(document.querySelector('#phist').textContent)"));
check("history shows the exact message that was sent", await js("/Hello Ayesha, this is a delivery check/.test(document.querySelector('#phist').textContent)"));
await shot("9-person");

console.log("== deliveries");
await js("document.querySelector('nav button[data-v=campaigns]').click()");
await until("!!document.querySelector('#clist button[data-deliv]')");
await js("[...document.querySelectorAll('#clist tr')].find(r => /Winter sale/.test(r.textContent)).querySelector('button[data-deliv]').click()");
check("campaign 'deliveries' opens the feed filtered to it", await until("document.querySelector('#fcp')?.selectedOptions[0]?.textContent === 'Winter sale'"));
check("summary cards show a sent count", await until("/[1-9]/.test(document.querySelector('#dsum .value')?.textContent || '')"));
check("feed has rows", await until("document.querySelectorAll('#dfeed tr:not([data-evrow])').length > 5"));
await js("const s=document.querySelector('#fs'); s.value='failed'; s.dispatchEvent(new Event('change'))");
check("failed filter shows only failed rows, each with a reason", await until("(() => { const rows = [...document.querySelectorAll('#dfeed tr:not([data-evrow])')].slice(1); return rows.length > 0 && rows.every(r => /failed/.test(r.textContent) && /invalid recipient|retries exhausted/.test(r.textContent)); })()"));
check("top failure reasons listed", await js("/invalid recipient/.test(document.querySelector('#dreasons').textContent)"));
await js("document.querySelector('#dfeed button[data-ev]').click()");
check("'attempts' expands the event timeline", await until("[...document.querySelectorAll('#dfeed tr[data-evrow]')].some(r => !r.hidden && /attempt 1/.test(r.textContent))"));
await shot("10-deliveries");
await js("document.querySelector('#dfeed button[data-person]').click()");
check("user link opens that person", await until("/Notifications \\(last 30 days\\)/.test(document.querySelector('#pdetail')?.textContent || '')"));

console.log("== system");
await js("document.querySelector('nav button[data-v=system]').click()");
check("Kafka topics table lists all 17 topics", await until("document.querySelectorAll('#stopics tr').length === 18"));
check("each topic shows its reader group", await js("[...document.querySelectorAll('#stopics tr')].some(r => /notify\\.email\\.bulk/.test(r.textContent) && /notify-workers-bulk/.test(r.textContent))"));
// After a worker restart, Kafka rebalances groups for up to ~45 s.
check("consumer groups shown Stable", await until("(document.querySelector('#sgroups').textContent.match(/Stable/g) || []).length === 5", 60000));
check("services table shows healthy", await until("/healthy/.test(document.querySelector('#ssvc').textContent) && !/down/.test(document.querySelector('#ssvc').textContent)"));
check("providers table shows each channel's provider", await until("/push/.test(document.querySelector('#sprov').textContent) && /mock|live|validate/.test(document.querySelector('#sprov').textContent)"));
check("storage shows Redis and Postgres", await js("/Redis memory/.test(document.querySelector('#sstore').textContent) && /Delivery history/.test(document.querySelector('#sstore').textContent)"));
check("dead-letter rows show reasons", await until("document.querySelectorAll('#sdlq tr').length > 10 && /invalid recipient|retries exhausted/.test(document.querySelector('#sdlq').textContent)"));
check("write rates measured after a refresh", await until("!/measuring/.test(document.querySelector('#stopics').textContent)", 12000));
await shot("11-system");

console.log("== users (admin)");
await js("document.querySelector('nav button[data-v=users]').click()");
check("users list shows me as (you)", await until(`/\(you\)/.test(document.querySelector('#ulist')?.textContent || '')`));
const NEWU = "ui-marketer-" + Date.now() + "@example.com";
await js(`const f=document.querySelector('#uf'); f.email.value='${NEWU}'; f.name.value='UI Marketer'; document.querySelector('#gen').click(); f.requestSubmit()`);
check("add user shows a one-time temporary password", await until(`document.querySelector('#ures').textContent.includes('${NEWU}') && /[A-Za-z2-9]{16}/.test(document.querySelector('#ures code')?.textContent || '')`));
const TEMP = await js("document.querySelector('#ures code').textContent");
check("new user appears as active marketer", await until(`[...document.querySelectorAll('#ulist tr')].some(r => r.textContent.includes('${NEWU}') && /marketer/.test(r.textContent) && /active/.test(r.textContent))`));
await shot("7-users");

console.log("== account: change password, sign out, sign back in");
await js("document.querySelector('#acct').click()");
await until("!!document.querySelector('#pf')");
const NEWPW = "Changed-In-UI-" + Date.now();
await js(`const f=document.querySelector('#pf'); f.cur.value=${JSON.stringify(PW)}; f.n1.value='${NEWPW}'; f.n2.value='nope-mismatch'; f.requestSubmit()`);
check("mismatched new passwords caught", await until("[...document.querySelectorAll('.toast')].some(t => /don't match/.test(t.textContent))"));
await js(`const f=document.querySelector('#pf'); f.n2.value='${NEWPW}'; f.requestSubmit()`);
check("password changed", await until("[...document.querySelectorAll('.toast')].some(t => /Password changed/.test(t.textContent))"));
await shot("8-account");
await js("document.querySelector('#logout').click()");
check("sign out returns to the sign-in form", await until("!!document.querySelector('#em')"));
await signIn(EMAIL, NEWPW);
check("signs in with the new password", await until("document.querySelectorAll('nav button').length === 9"));
await js("document.querySelector('#logout').click()"); await until("!!document.querySelector('#em')");
await signIn(NEWU, TEMP);
check("new marketer signs in with temp password; no Users tab", await until("document.querySelectorAll('nav button').length === 8 && !/Users/.test(document.querySelector('nav').textContent)"));
errors.length = 0;

console.log("== phone width (400px)");
await cdp("Emulation.setDeviceMetricsOverride", { width: 400, height: 860, deviceScaleFactor: 2, mobile: true });
await js("document.querySelector('nav button[data-v=overview]').click()"); await sleep(1500);
const sw = await js("document.documentElement.scrollWidth");
check("no horizontal page scroll at 400px", sw <= 400, `scrollWidth=${sw}`);
check("mobile: nav on its own full-width row, scrollable if needed", await js("(() => { const n = document.querySelector('nav'); return (n.scrollWidth <= n.clientWidth + 1 || getComputedStyle(n).overflowX === 'auto') && n.getBoundingClientRect().width >= 360; })()"));
await shot("6-mobile");

console.log("== console");
check("no JavaScript errors", errors.length === 0, errors.slice(0, 3).join(" | "));
console.log(`\nUI RESULT: ${PASS} passed, ${FAIL} failed`);
ws.close(); process.exit(0);
