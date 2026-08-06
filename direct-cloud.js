(function(){
'use strict';

/*
 * CashTop direct cloud adapter.
 * This build intentionally contains the user's Turso/Bunny credentials so the
 * static site works without installing a Worker/server. Do not publish this
 * file publicly unless you accept that browser users can inspect those keys.
 */
const CFG={
  TURSO_DATABASE_URL:'libsql://cash-top-homworkhhh76-rgb.aws-eu-west-1.turso.io',
  TURSO_AUTH_TOKEN:'eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3ODUwODYwNTIsImlkIjoiMDE5ZjlmNjYtOTQwMS03MmEwLTkyNzItYjVhZjA2ODczZmIyIiwia2lkIjoicVgzS01DZ0pwQnp3eGo1Tzl2SHhaWUJGem9sTWFsa24tTU5JOTRlMTl6YyIsInJpZCI6ImQxZmE2MjhjLThiYTMtNDJhNS04MzhmLTc1MGJhNGQwYWE1YiJ9.Dl9BkY70zPZCzGnf_MHg2A7GtWsnd6BRGQoUyEeEPIz3BWbkDj70xD-B7x5U5VG8aBoiljNtCpg0OHJCjnuoAA',
  BUNNY_STORAGE_ZONE:'amanwar1',
  BUNNY_PULL_ZONE_URL:'https://amanwar1.b-cdn.net',
  BUNNY_ACCESS_KEY:'bd094c93-3387-44e5-8ee02b4ff7c3-f22d-4060',
  BUNNY_ROOT_FOLDER:'cashtop-images',
  ADMIN_PASSWORD:'admin123',
  SESSION_SECRET:'cashtop-direct-v5-session-2026-08-05-please-change-if-public',
  LICENSE_PEPPER:'change-this-to-a-second-long-random-secret-please'
};

const MAX_PUSH_OPS=150,MAX_PULL=500,MAX_IMAGE_BYTES=50*1024;
let schemaPromise=null;
let companyCache=new Map();

function err(status,message){const e=new Error(message);e.status=status;return e}
function tursoEndpoint(){return CFG.TURSO_DATABASE_URL.replace(/^libsql:\/\//i,'https://').replace(/\/+$/,'')+'/v2/pipeline'}
function tursoArg(v){if(v===null||v===undefined)return{type:'null'};if(typeof v==='number')return Number.isInteger(v)?{type:'integer',value:String(v)}:{type:'float',value:String(v)};if(typeof v==='boolean')return{type:'integer',value:v?'1':'0'};return{type:'text',value:String(v)}}
function stmt(sql,args=[]){return{type:'execute',stmt:{sql,args:args.map(tursoArg)}}}
async function pipeline(statements){
  const res=await fetch(tursoEndpoint(),{method:'POST',headers:{Authorization:'Bearer '+CFG.TURSO_AUTH_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({requests:[...statements,{type:'close'}]}),cache:'no-store'});
  const text=await res.text();let data=null;try{data=JSON.parse(text)}catch{}
  if(!res.ok||!data)throw err(502,'تعذر الاتصال بقاعدة Turso ('+res.status+')');
  const results=data.results||[];
  for(let i=0;i<statements.length;i++)if(results[i]?.type!=='ok')throw err(500,results[i]?.error?.message||'خطأ في قاعدة Turso');
  return results.slice(0,statements.length).map(x=>x.response?.result||{});
}
function rowsOf(result){const cols=(result.cols||[]).map(c=>c.name);return(result.rows||[]).map(row=>{const o={};row.forEach((cell,i)=>{let v=cell?.value??null;if(cell?.type==='integer'||cell?.type==='float')v=Number(v);o[cols[i]]=v});return o})}
async function sha256Hex(text){const b=new TextEncoder().encode(String(text));const d=await crypto.subtle.digest('SHA-256',b);return[...new Uint8Array(d)].map(x=>x.toString(16).padStart(2,'0')).join('')}
async function licenseHash(key){return sha256Hex(String(key||'').trim().toUpperCase()+'|'+CFG.LICENSE_PEPPER)}
function b64uText(text){const bytes=new TextEncoder().encode(text);let s='';bytes.forEach(b=>s+=String.fromCharCode(b));return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function b64uBytes(bytes){let s='';bytes.forEach(b=>s+=String.fromCharCode(b));return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
function b64uDecode(s){s=String(s).replace(/-/g,'+').replace(/_/g,'/');while(s.length%4)s+='=';const bin=atob(s);return new Uint8Array([...bin].map(c=>c.charCodeAt(0)))}
async function signToken(payload){const body=b64uText(JSON.stringify(payload));const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(CFG.SESSION_SECRET),{name:'HMAC',hash:'SHA-256'},false,['sign']);const sig=new Uint8Array(await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(body)));return body+'.'+b64uBytes(sig)}
async function verifyToken(token){const [body,sig]=String(token||'').split('.');if(!body||!sig)throw err(401,'جلسة غير صالحة');const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(CFG.SESSION_SECRET),{name:'HMAC',hash:'SHA-256'},false,['verify']);const ok=await crypto.subtle.verify('HMAC',key,b64uDecode(sig),new TextEncoder().encode(body));if(!ok)throw err(401,'جلسة غير صالحة');const p=JSON.parse(new TextDecoder().decode(b64uDecode(body)));if(!p.exp||Date.now()>=Number(p.exp))throw err(401,'انتهت الجلسة');return p}

async function ensureSchema(){
  if(schemaPromise)return schemaPromise;
  schemaPromise=(async()=>{
    await pipeline([
      stmt(`CREATE TABLE IF NOT EXISTS ct3_companies (id TEXT PRIMARY KEY,name TEXT NOT NULL,key_hash TEXT NOT NULL UNIQUE,key_hint TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,deleted_at INTEGER)`),
      stmt(`CREATE INDEX IF NOT EXISTS ct3_companies_status_exp ON ct3_companies(status,expires_at)`),
      stmt(`CREATE TABLE IF NOT EXISTS ct3_sync_items (company_id TEXT NOT NULL,kind TEXT NOT NULL,id TEXT NOT NULL,parent_id TEXT NOT NULL DEFAULT '',sort_ts INTEGER NOT NULL DEFAULT 0,payload_json TEXT NOT NULL DEFAULT '{}',deleted INTEGER NOT NULL DEFAULT 0,server_updated_at INTEGER NOT NULL,last_op_id TEXT NOT NULL DEFAULT '',PRIMARY KEY(company_id,kind,id))`),
      stmt(`CREATE INDEX IF NOT EXISTS ct3_sync_changes ON ct3_sync_items(company_id,server_updated_at)`),
      stmt(`CREATE INDEX IF NOT EXISTS ct3_sync_list ON ct3_sync_items(company_id,kind,parent_id,deleted,sort_ts DESC,id DESC)`),
      stmt(`CREATE TABLE IF NOT EXISTS ct3_public_customer_links (public_key TEXT PRIMARY KEY,company_id TEXT NOT NULL,person_key TEXT NOT NULL,person_type TEXT NOT NULL,person_name TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,UNIQUE(company_id,person_key))`),
      stmt(`CREATE INDEX IF NOT EXISTS ct3_public_customer_company ON ct3_public_customer_links(company_id,person_key,active)`)
    ]);

    // لا يوجد مفتاح شركة افتراضي. يتم إنشاء المفاتيح من ملف admin.html فقط.
    // تنظيف المفتاح التجريبي القديم إن كان موجوداً من الإصدارات السابقة.
    await pipeline([
      stmt(`DELETE FROM ct3_public_customer_links WHERE company_id='default-company'`),
      stmt(`DELETE FROM ct3_sync_items WHERE company_id='default-company'`),
      stmt(`DELETE FROM ct3_companies WHERE id='default-company'`)
    ]);
    return true;
  })().catch(e=>{schemaPromise=null;throw e});
  return schemaPromise;
}

async function requireCompany(token,force=false){
  const p=await verifyToken(token);if(p.type!=='company'||!p.cid)throw err(401,'تسجيل الدخول مطلوب');
  const cached=companyCache.get(p.cid);if(!force&&cached&&Date.now()-cached.at<60000)return cached.company;
  const [r]=await pipeline([stmt(`SELECT id,name,status,expires_at,updated_at FROM ct3_companies WHERE id=? LIMIT 1`,[p.cid])]);const c=rowsOf(r)[0];
  if(!c||c.status==='deleted')throw err(401,'مفتاح الشركة محذوف');if(c.status!=='active')throw err(401,'مفتاح الشركة متوقف');if(Date.now()>=Number(c.expires_at))throw err(401,'انتهت مدة مفتاح الشركة');
  companyCache.set(p.cid,{at:Date.now(),company:c});return c;
}
async function requireAdmin(token){const p=await verifyToken(token);if(p.type!=='admin')throw err(403,'صلاحيات الأدمن مطلوبة');return p}
function normalizeExpiry(expiresAt,durationDays){let t=Number(expiresAt||0);if(!t&&durationDays!==undefined)t=Date.now()+Math.max(1,Number(durationDays)||1)*86400000;if(!t)t=Date.now()+30*86400000;if(t<=Date.now())throw err(400,'تاريخ الانتهاء يجب أن يكون في المستقبل');return Math.floor(t)}
function randomCompanyKey(){const a='ABCDEFGHJKLMNPQRSTUVWXYZ23456789',b=crypto.getRandomValues(new Uint8Array(12)),parts=[];for(let p=0;p<3;p++){let s='';for(let i=0;i<4;i++)s+=a[b[p*4+i]%a.length];parts.push(s)}return'CT-'+parts.join('-')}
function randomPublicKey(){const a='ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789',b=crypto.getRandomValues(new Uint8Array(11));let s='';for(let i=0;i<b.length;i++)s+=a[b[i]%a.length];return s}
function parseSyncRow(r){let payload={};try{payload=JSON.parse(r.payload_json||'{}')}catch{}return{rowid:Number(r.rowid||0),kind:r.kind,id:r.id,parentId:r.parent_id||'',sortTs:Number(r.sort_ts||0),payload,deleted:Boolean(Number(r.deleted||0)),serverUpdatedAt:Number(r.server_updated_at||0)}}
function validateOp(op){const kind=String(op.kind||'').slice(0,60),id=String(op.id||'').slice(0,180);if(!kind||!id)return null;const parentId=String(op.parentId||'').slice(0,200),sortTs=Number(op.sortTs||0)||0,deleted=op.deleted?1:0,opId=String(op.opId||crypto.randomUUID()).slice(0,180),clean=deleted?{}:{...(op.payload||{})};delete clean.imageLocalId;if(String(clean.image||'').startsWith('data:image/'))throw err(413,'الصورة يجب رفعها إلى Bunny أولاً');if(String(clean.imageUrl||'').startsWith('data:image/'))throw err(413,'الصورة يجب رفعها إلى Bunny أولاً');const payloadJson=deleted?'{}':JSON.stringify(clean);if(payloadJson.length>450000)throw err(413,'السجل كبير جداً');return{kind,id,parentId,sortTs,deleted,opId,payloadJson}}
function tokenFrom(opt){return String(opt?.token||'')}

async function request(path,opt={}){
  await ensureSchema();
  const u=new URL('https://local.invalid'+path),p=u.pathname,method=String(opt.method||'GET').toUpperCase(),body=opt.body||{},token=tokenFrom(opt);
  if(p==='/health')return{ok:true,time:Date.now(),service:'CashTop Direct v5'};
  if(p==='/auth/login'&&method==='POST'){
    const key=String(body.companyKey||'').trim().toUpperCase();if(!key)throw err(400,'أدخل مفتاح الشركة');const hash=await licenseHash(key);const[r]=await pipeline([stmt(`SELECT id,name,key_hint,status,expires_at,updated_at FROM ct3_companies WHERE key_hash=? LIMIT 1`,[hash])]);const c=rowsOf(r)[0];if(!c||c.status==='deleted')throw err(401,'مفتاح الشركة غير صحيح');if(c.status!=='active')throw err(401,'مفتاح الشركة متوقف');if(Date.now()>=Number(c.expires_at))throw err(401,'انتهت مدة مفتاح الشركة');const t=await signToken({type:'company',cid:c.id,exp:Number(c.expires_at)});return{ok:true,token:t,company:{id:c.id,name:c.name,expiresAt:Number(c.expires_at),status:c.status}};
  }
  if(p==='/auth/validate'&&method==='GET'){const c=await requireCompany(token,true);return{ok:true,company:{id:c.id,name:c.name,expiresAt:Number(c.expires_at),status:c.status},serverTime:Date.now()}}
  if(p==='/admin/login'&&method==='POST'){if(String(body.password||'')!==CFG.ADMIN_PASSWORD)throw err(401,'كلمة مرور الأدمن غير صحيحة');const exp=Date.now()+12*3600000;return{ok:true,token:await signToken({type:'admin',exp}),expiresAt:exp}}
  if(p==='/admin/companies'&&method==='GET'){await requireAdmin(token);const[r]=await pipeline([stmt(`SELECT id,name,key_hint,status,expires_at,created_at,updated_at,deleted_at FROM ct3_companies ORDER BY created_at DESC LIMIT 500`)]);return{ok:true,companies:rowsOf(r)}}
  if(p==='/admin/companies'&&method==='POST'){await requireAdmin(token);const name=String(body.name||'').trim();if(!name)throw err(400,'اسم الشركة مطلوب');const plainKey=String(body.companyKey||randomCompanyKey()).trim().toUpperCase(),expiresAt=normalizeExpiry(body.expiresAt,body.durationDays),now=Date.now(),id=crypto.randomUUID(),hash=await licenseHash(plainKey),hint=plainKey.length>8?plainKey.slice(0,5)+'••••'+plainKey.slice(-4):'••••';try{await pipeline([stmt(`INSERT INTO ct3_companies(id,name,key_hash,key_hint,status,expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,[id,name,hash,hint,'active',expiresAt,now,now])])}catch(e){if(/UNIQUE|constraint/i.test(String(e.message)))throw err(409,'مفتاح الشركة مستخدم مسبقاً');throw e}return{ok:true,company:{id,name,companyKey:plainKey,keyHint:hint,status:'active',expiresAt,createdAt:now}}}
  const m=p.match(/^\/admin\/companies\/([^/]+)$/);if(m&&(method==='PATCH'||method==='DELETE')){await requireAdmin(token);const id=decodeURIComponent(m[1]),now=Date.now();if(method==='DELETE'){
    const[q0]=await pipeline([stmt(`SELECT id FROM ct3_companies WHERE id=? LIMIT 1`,[id])]);
    if(!rowsOf(q0)[0])throw err(404,'الشركة غير موجودة');
    await pipeline([
      stmt(`DELETE FROM ct3_public_customer_links WHERE company_id=?`,[id]),
      stmt(`DELETE FROM ct3_sync_items WHERE company_id=?`,[id]),
      stmt(`DELETE FROM ct3_companies WHERE id=?`,[id])
    ]);
    companyCache.delete(id);return{ok:true,deletedData:true}
  }const[q]=await pipeline([stmt(`SELECT id,name,status,expires_at FROM ct3_companies WHERE id=? LIMIT 1`,[id])]);const old=rowsOf(q)[0];if(!old)throw err(404,'الشركة غير موجودة');const name=body.name!==undefined?String(body.name).trim():old.name,status=body.status!==undefined?String(body.status):old.status;if(!['active','stopped','deleted'].includes(status))throw err(400,'حالة غير صالحة');let expiresAt=Number(old.expires_at);if(body.expiresAt!==undefined||body.durationDays!==undefined)expiresAt=normalizeExpiry(body.expiresAt,body.durationDays);await pipeline([stmt(`UPDATE ct3_companies SET name=?,status=?,expires_at=?,updated_at=?,deleted_at=? WHERE id=?`,[name,status,expiresAt,now,status==='deleted'?now:null,id])]);companyCache.delete(id);return{ok:true,company:{id,name,status,expiresAt,updatedAt:now}}}
  if(p==='/public-links/customer'&&method==='POST'){
    const c=await requireCompany(token),personKey=String(body.personKey||'').trim().slice(0,200),personType=String(body.type||'customers').trim().slice(0,30),personName=String(body.name||'').trim().slice(0,180),requested=String(body.publicKey||'').trim().slice(0,40),now=Date.now();
    if(!personKey||!personName)throw err(400,'بيانات العميل غير مكتملة');
    let existing=null;
    if(requested){const[q]=await pipeline([stmt(`SELECT public_key,company_id,person_key FROM ct3_public_customer_links WHERE public_key=? LIMIT 1`,[requested])]);const x=rowsOf(q)[0];if(x&&x.company_id===c.id)existing=x}
    if(!existing){const[q]=await pipeline([stmt(`SELECT public_key,company_id,person_key FROM ct3_public_customer_links WHERE company_id=? AND person_key=? LIMIT 1`,[c.id,personKey])]);existing=rowsOf(q)[0]||null}
    if(existing){await pipeline([stmt(`UPDATE ct3_public_customer_links SET person_key=?,person_type=?,person_name=?,active=1,updated_at=? WHERE public_key=? AND company_id=?`,[personKey,personType,personName,now,existing.public_key,c.id])]);return{ok:true,key:existing.public_key}}
    if(requested&&/^[A-Za-z0-9_-]{8,40}$/.test(requested)){try{await pipeline([stmt(`INSERT INTO ct3_public_customer_links(public_key,company_id,person_key,person_type,person_name,active,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,[requested,c.id,personKey,personType,personName,1,now,now])]);return{ok:true,key:requested}}catch(e){if(!/UNIQUE|constraint/i.test(String(e.message)))throw e}}
    for(let i=0;i<8;i++){
      const publicKey=randomPublicKey();
      try{await pipeline([stmt(`INSERT INTO ct3_public_customer_links(public_key,company_id,person_key,person_type,person_name,active,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,[publicKey,c.id,personKey,personType,personName,1,now,now])]);return{ok:true,key:publicKey}}catch(e){if(!/UNIQUE|constraint/i.test(String(e.message)))throw e}
    }
    throw err(500,'تعذر إنشاء رابط العميل');
  }
  if(p==='/public/customer'&&method==='GET'){
    const publicKey=String(u.searchParams.get('key')||'').trim().slice(0,40);if(!publicKey)throw err(400,'الرابط غير مكتمل');
    const[q]=await pipeline([stmt(`SELECT l.public_key,l.company_id,l.person_key,l.person_type,l.person_name,l.active,c.name AS company_name,c.status,c.expires_at FROM ct3_public_customer_links l JOIN ct3_companies c ON c.id=l.company_id WHERE l.public_key=? LIMIT 1`,[publicKey])]);
    const link=rowsOf(q)[0];if(!link||!Number(link.active)||link.status!=='active'||Date.now()>=Number(link.expires_at||0))throw err(404,'رابط العميل غير متاح');
    const beforeTsRaw=u.searchParams.get('beforeTs'),beforeTs=beforeTsRaw===null?Number.MAX_SAFE_INTEGER:Number(beforeTsRaw||0),beforeId=String(u.searchParams.get('beforeId')||'\uffff'),limit=Math.min(100,Math.max(10,Number(u.searchParams.get('limit')||40)));
    const [sumR,listR,contactR,profileR]=await pipeline([
      stmt(`SELECT COALESCE(SUM(CAST(COALESCE(json_extract(payload_json,'$.credit'),0) AS REAL)),0) AS total_credit,COALESCE(SUM(CAST(COALESCE(json_extract(payload_json,'$.debit'),0) AS REAL)),0) AS total_debit,COUNT(*) AS cnt FROM ct3_sync_items WHERE company_id=? AND kind='debt_record' AND parent_id=? AND deleted=0 AND (CAST(COALESCE(json_extract(payload_json,'$.debit'),0) AS REAL)>0 OR CAST(COALESCE(json_extract(payload_json,'$.credit'),0) AS REAL)>0)`,[link.company_id,link.person_key]),
      stmt(`SELECT id,sort_ts,payload_json FROM ct3_sync_items WHERE company_id=? AND kind='debt_record' AND parent_id=? AND deleted=0 AND (CAST(COALESCE(json_extract(payload_json,'$.debit'),0) AS REAL)>0 OR CAST(COALESCE(json_extract(payload_json,'$.credit'),0) AS REAL)>0) AND (sort_ts<? OR (sort_ts=? AND id<?)) ORDER BY sort_ts DESC,id DESC LIMIT ?`,[link.company_id,link.person_key,beforeTs,beforeTs,beforeId,limit]),
      stmt(`SELECT payload_json FROM ct3_sync_items WHERE company_id=? AND kind='contact' AND id=? AND deleted=0 LIMIT 1`,[link.company_id,link.person_key]),
      stmt(`SELECT payload_json FROM ct3_sync_items WHERE company_id=? AND kind='profile' AND id='main' AND deleted=0 LIMIT 1`,[link.company_id])
    ]);
    const summary=rowsOf(sumR)[0]||{},list=rowsOf(listR).map(r=>{let x={};try{x=JSON.parse(r.payload_json||'{}')}catch{}return{...x,id:r.id,timestamp:Number(r.sort_ts||x.timestamp||0)}});
    let contact={},profile={};try{contact=JSON.parse(rowsOf(contactR)[0]?.payload_json||'{}')}catch{}try{profile=JSON.parse(rowsOf(profileR)[0]?.payload_json||'{}')}catch{}
    const totalCredit=Number(summary.total_credit||0),totalDebit=Number(summary.total_debit||0),last=list[list.length-1];
    return{ok:true,customer:{name:link.person_name,type:link.person_type,phone:contact.phone||''},company:{name:profile.name||link.company_name||'كاش توب',phone:profile.phone||'',address:profile.address||''},summary:{balance:totalCredit-totalDebit,totalCredit,totalDebit,count:Number(summary.cnt||0)},transactions:list,cursor:last?{beforeTs:Number(last.timestamp||0),beforeId:String(last.id)}:null,hasMore:list.length===limit};
  }
  if(p==='/sync/push'&&method==='POST'){const c=await requireCompany(token),incoming=Array.isArray(body.ops)?body.ops.slice(0,MAX_PUSH_OPS):[];if(!incoming.length)return{ok:true,acked:[],serverTime:Date.now()};const now=Date.now(),valid=incoming.map(validateOp).filter(Boolean),ss=valid.map((op,i)=>stmt(`INSERT INTO ct3_sync_items(company_id,kind,id,parent_id,sort_ts,payload_json,deleted,server_updated_at,last_op_id) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(company_id,kind,id) DO UPDATE SET parent_id=excluded.parent_id,sort_ts=excluded.sort_ts,payload_json=excluded.payload_json,deleted=excluded.deleted,server_updated_at=excluded.server_updated_at,last_op_id=excluded.last_op_id WHERE ct3_sync_items.last_op_id <> excluded.last_op_id`,[c.id,op.kind,op.id,op.parentId,op.sortTs,op.payloadJson,op.deleted,now+i,op.opId]));await pipeline(ss);return{ok:true,acked:valid.map(x=>x.opId),serverTime:now+valid.length}}
  if(p==='/sync/pull'&&method==='GET'){const c=await requireCompany(token),t=Math.max(0,Number(u.searchParams.get('t')||0)),rowid=Math.max(0,Number(u.searchParams.get('rowid')||0)),limit=Math.min(MAX_PULL,Math.max(1,Number(u.searchParams.get('limit')||250)));const[r]=await pipeline([stmt(`SELECT rowid,kind,id,parent_id,sort_ts,payload_json,deleted,server_updated_at FROM ct3_sync_items WHERE company_id=? AND kind NOT IN ('archive_record','debt_archive_record') AND (server_updated_at>? OR (server_updated_at=? AND rowid>?)) ORDER BY server_updated_at ASC,rowid ASC LIMIT ?`,[c.id,t,t,rowid,limit])]);const rows=rowsOf(r).map(parseSyncRow),last=rows[rows.length-1];return{ok:true,items:rows,cursor:last?{t:last.serverUpdatedAt,rowid:last.rowid}:{t,rowid},hasMore:rows.length===limit}}
  if(p==='/items/debt-archive-summary'&&method==='GET'){
    const c=await requireCompany(token),parentId=String(u.searchParams.get('parentId')||'').slice(0,200);if(!parentId)throw err(400,'parentId مطلوب');
    const[r]=await pipeline([stmt(`SELECT COALESCE(SUM(CAST(COALESCE(json_extract(payload_json,'$.credit'),0) AS REAL)),0) AS total_credit,COALESCE(SUM(CAST(COALESCE(json_extract(payload_json,'$.debit'),0) AS REAL)),0) AS total_debit,COUNT(*) AS cnt FROM ct3_sync_items WHERE company_id=? AND kind='debt_archive_record' AND parent_id=? AND deleted=0`,[c.id,parentId])]);const x=rowsOf(r)[0]||{};const totalCredit=Number(x.total_credit||0),totalDebit=Number(x.total_debit||0);return{ok:true,totalCredit,totalDebit,balance:totalCredit-totalDebit,count:Number(x.cnt||0)};
  }
  if(p==='/items/list'&&method==='GET'){const c=await requireCompany(token),kind=String(u.searchParams.get('kind')||'').slice(0,60),parentId=String(u.searchParams.get('parentId')||'').slice(0,200);if(!kind)throw err(400,'kind مطلوب');const beforeTsRaw=u.searchParams.get('beforeTs'),beforeTs=beforeTsRaw===null?Number.MAX_SAFE_INTEGER:Number(beforeTsRaw||0),beforeId=String(u.searchParams.get('beforeId')||'\uffff'),limit=Math.min(120,Math.max(1,Number(u.searchParams.get('limit')||50)));const[r]=await pipeline([stmt(`SELECT rowid,kind,id,parent_id,sort_ts,payload_json,deleted,server_updated_at FROM ct3_sync_items WHERE company_id=? AND kind=? AND parent_id=? AND deleted=0 AND (sort_ts<? OR (sort_ts=? AND id<?)) ORDER BY sort_ts DESC,id DESC LIMIT ?`,[c.id,kind,parentId,beforeTs,beforeTs,beforeId,limit])]);const rows=rowsOf(r).map(parseSyncRow);return{ok:true,items:rows,hasMore:rows.length===limit}}
  if(p==='/images/upload'&&method==='POST'){
    const c=await requireCompany(token),fd=opt.form,file=fd?.get?.('file');if(!(file instanceof Blob))throw err(400,'ملف الصورة مطلوب');if(file.size>MAX_IMAGE_BYTES)throw err(413,'يجب ضغط الصورة إلى 50KB أو أقل قبل الرفع');if(!/^image\/(jpeg|jpg)$/i.test(file.type||''))throw err(415,'الصورة يجب أن تكون JPEG بعد الضغط');const safe=(v,f='item')=>(String(v||'').trim().replace(/[^a-zA-Z0-9_-]+/g,'_').replace(/^_+|_+$/g,'')||f).slice(0,80),folder=safe(fd.get('folder')||'misc','misc'),entity=safe(fd.get('entityId')||'item','item'),root=safe(CFG.BUNNY_ROOT_FOLDER,'cashtop-images'),tenant=safe(c.id,'company'),path=`${root}/${tenant}/${folder}/${entity}_${Date.now()}_${crypto.randomUUID().slice(0,8)}.jpg`,endpoint=`https://storage.bunnycdn.com/${encodeURIComponent(CFG.BUNNY_STORAGE_ZONE)}/${path.split('/').map(encodeURIComponent).join('/')}`;const r=await fetch(endpoint,{method:'PUT',headers:{AccessKey:CFG.BUNNY_ACCESS_KEY,'Content-Type':'image/jpeg'},body:file});if(!r.ok)throw err(502,'فشل رفع الصورة إلى Bunny ('+r.status+')');return{ok:true,url:CFG.BUNNY_PULL_ZONE_URL.replace(/\/+$/,'')+'/'+path.split('/').map(encodeURIComponent).join('/'),size:file.size,storage:'bunny'};
  }
  if(p==='/images/delete'&&method==='POST'){
    const c=await requireCompany(token),raw=String(body.url||'').trim();if(!raw)return{ok:true,deleted:false,reason:'empty'};
    let url;try{url=new URL(raw)}catch{return{ok:true,deleted:false,reason:'invalid'}}
    const pull=new URL(CFG.BUNNY_PULL_ZONE_URL),safe=(v,f='item')=>(String(v||'').trim().replace(/[^a-zA-Z0-9_-]+/g,'_').replace(/^_+|_+$/g,'')||f).slice(0,80),root=safe(CFG.BUNNY_ROOT_FOLDER,'cashtop-images'),tenant=safe(c.id,'company');
    if(url.hostname!==pull.hostname)return{ok:true,deleted:false,reason:'external'};
    let path='';try{path=url.pathname.replace(/^\/+/, '').split('/').map(x=>decodeURIComponent(x)).join('/')}catch{return{ok:true,deleted:false,reason:'invalid-path'}}
    const prefix=`${root}/${tenant}/`;if(!path.startsWith(prefix)||path.includes('..'))throw err(403,'لا يمكن حذف صورة خارج مساحة الشركة');
    const endpoint=`https://storage.bunnycdn.com/${encodeURIComponent(CFG.BUNNY_STORAGE_ZONE)}/${path.split('/').map(encodeURIComponent).join('/')}`;
    const r=await fetch(endpoint,{method:'DELETE',headers:{AccessKey:CFG.BUNNY_ACCESS_KEY}});if(!r.ok&&r.status!==404)throw err(502,'فشل حذف الصورة من Bunny ('+r.status+')');return{ok:true,deleted:r.status!==404,url:raw};
  }
  throw err(404,'المسار غير موجود');
}

window.CashTopDirectAPI={request,ensureSchema,config:{database:CFG.TURSO_DATABASE_URL,bunny:CFG.BUNNY_PULL_ZONE_URL,mode:'direct'}};
})();
