(function(){
'use strict';

const LINK_KEY='cashtop2_link_v1';
const STATE_KEY='cashtop2_link_state_v1';
const POS_ROOT='cashTopExchange/cashTopPOS';
const ADMIN_ROOT='cashTopExchange/cashTopAdmin';
const LEGACY_GROUP='FG_LEGACY';
const GROUP_SCOPED=new Set([
  'cashtop_products','cashtop_product_categories','cashtop_materials','cashtop_material_purchases',
  'cashtop_customers','cashtop_suppliers','cashtop_supplier_movements','cashtop_invoices','cashtop_sales_reversals',
  'cashtop_sales_returns','cashtop_purchases','cashtop_purchase_reversals','cashtop_purchase_returns','cashtop_expenses',
  'cashtop_funds_db','cashtop_vouchers','cashtop_transfer_history','cashtop_branch_transfer_history','cashtop_workers',
  'cashtop_sales_agents','cashtop_agent_movements','cashtop_journal','cashtop_journal_reversal_archive','cashtop_audit_log',
  'cashtop_manufacturing_orders','cashtop_wastage','cashtop_archive_index','cashtop_salary_payments','cashtop_opening_balances'
]);
let syncing=false, syncTimer=null, autoTimer=null;

const clone=v=>{try{return structuredClone(v)}catch(_){return JSON.parse(JSON.stringify(v))}};
const num=v=>Number.isFinite(Number(v))?Number(v):0;
const clean=s=>String(s??'').trim();
const safeSeg=v=>clean(v).replace(/[.#$\[\]\/]/g,'_');
const normKey=v=>clean(v).toUpperCase();
const normUser=v=>clean(v).toLowerCase();
const scoped=k=>window.ctScopedKey?window.ctScopedKey(k):k;
function load(k,f){try{const x=localStorage.getItem(scoped(k));return x===null?f:JSON.parse(x)}catch(_){return f}}
function save(k,v){try{localStorage.setItem(scoped(k),JSON.stringify(v))}catch(e){console.warn('[CashTopLink] storage',e)}}
function link(){return load(LINK_KEY,null)}
function state(){const s=load(STATE_KEY,{});return {seenCash:s.seenCash||{},seenParty:s.seenParty||{},observedBalances:s.observedBalances||{},pushedLocal:s.pushedLocal||{},initializedContacts:s.initializedContacts||{},tombstones:s.tombstones||{},pendingDeletes:s.pendingDeletes||{},remoteInvoices:s.remoteInvoices||{},remoteFundLogs:s.remoteFundLogs||{},remoteVouchers:s.remoteVouchers||{},remoteSupplierMovements:s.remoteSupplierMovements||{},lastMetaUpdatedAt:num(s.lastMetaUpdatedAt),lastSyncAt:num(s.lastSyncAt),...s}}
function saveState(s){save(STATE_KEY,s)}
function toastMsg(t,icon='success'){try{toast(t,icon)}catch(_){console.log(t)}}
function canWrite(show=true){return window.CashTopSync?.canWrite?window.CashTopSync.canWrite(show):true}
function isExpired(){return window.CashTopSync?.isSubscriptionExpired?.()===true}
function nowIso(){return new Date().toISOString()}
function dateTs(v){if(v==null||v==='')return Date.now();if(v instanceof Date){const t=v.getTime();return Number.isFinite(t)?t:Date.now()}if(typeof v==='number'){if(!Number.isFinite(v))return Date.now();return v>1e9&&v<1e12?v*1000:v}let raw=clean(v);if(!raw)return Date.now();raw=raw.replace(/[٠-٩]/g,d=>'٠١٢٣٤٥٦٧٨٩'.indexOf(d));const n=Number(raw);if(Number.isFinite(n)&&n>1e9)return n<1e12?n*1000:n;let t=Date.parse(raw);if(!Number.isFinite(t)&&/^\d{4}-\d{2}-\d{2}$/.test(raw))t=Date.parse(raw+'T12:00:00');return Number.isFinite(t)?t:Date.now()}
function recordId(prefix,ref){let h=2166136261;for(const ch of String(ref)){h^=ch.charCodeAt(0);h=Math.imul(h,16777619)}return `${prefix}_${(h>>>0).toString(36)}_${Date.now().toString(36)}`}
function entityKey(kind,id){return `${clean(kind)}:${clean(id)}`}
function isTombstoned(s,kind,id){return !!s?.tombstones?.[entityKey(kind,id)]}
function markTombstone(s,kind,id,origin='sync'){if(!id)return;const k=entityKey(kind,id);s.tombstones=s.tombstones||{};s.tombstones[k]={at:Date.now(),origin,kind:clean(kind),id:clean(id)}}
function invoiceCustomerName(inv,log){return clean(inv?.customer||inv?.customerName||log?.relationName)||'عميل نقدي'}
function invoiceTimestamp(inv,log){return dateTs(inv?.date||inv?.createdAt||inv?.updatedAt||log?.createdAt||log?.updatedAt||log?.date)}
function invoiceSnapshot(inv){if(!inv||typeof inv!=='object')return null;try{return clone(inv)}catch(_){return {...inv}}}
function sourceMeta(sourceType,sourceId,extra={}){return {cashTop2SourceType:clean(sourceType),cashTop2SourceId:clean(sourceId),...extra}}

function decodeStored(value,fallback=null){
  let parsed=value;
  for(let i=0;i<4&&typeof parsed==='string';i++){
    try{parsed=JSON.parse(parsed)}catch(_){break}
  }
  if(parsed&&typeof parsed==='object'&&!Array.isArray(parsed)&&Object.prototype.hasOwnProperty.call(parsed,'value')&&
     (parsed.valueEncoding||Object.prototype.hasOwnProperty.call(parsed,'deleted')||Object.prototype.hasOwnProperty.call(parsed,'updatedAt'))){
    if(parsed.deleted===true)return fallback;
    return decodeStored(parsed.value,fallback);
  }
  return parsed==null?fallback:parsed;
}
function normalizeArray(value){
  const v=decodeStored(value,[]);
  if(Array.isArray(v))return v.filter(Boolean);
  if(v&&typeof v==='object'){
    const hints=['id','name','username','companyKey','tenantId','companyId','role','branchId'];
    if(hints.some(k=>Object.prototype.hasOwnProperty.call(v,k)))return [v];
    return Object.entries(v).map(([id,x])=>{x=decodeStored(x,null);return x&&typeof x==='object'&&!Array.isArray(x)?(x.id==null?{...x,id}:x):null}).filter(Boolean);
  }
  return [];
}
function actualObject(value,fallback={}){const v=decodeStored(value,fallback);return v&&typeof v==='object'&&!Array.isArray(v)?v:fallback}
function physicalKey(logical,groupId){return GROUP_SCOPED.has(logical)&&groupId&&groupId!==LEGACY_GROUP?`fg_${safeSeg(groupId)}__${logical}`:logical}
function stampKey(logical,groupId){return GROUP_SCOPED.has(logical)&&groupId&&groupId!==LEGACY_GROUP?`fg:${groupId}:${logical}`:logical}
function datasetPath(tenant,logical,groupId){return `${POS_ROOT}/${safeSeg(tenant)}/datasets/${safeSeg(physicalKey(logical,groupId))}`}
function metaPath(tenant){return `${POS_ROOT}/${safeSeg(tenant)}/meta`}

async function readPath(path){if(!window.CashTopDirectAPI?.rtdbRead)throw new Error('مكوّن الربط غير متاح');return CashTopDirectAPI.rtdbRead(path)}
async function readDataset(tenant,logical,groupId,fallback){const row=await readPath(datasetPath(tenant,logical,groupId));return {row,data:decodeStored(row.value,fallback)}}
function makeWrapper(previous,next,page='daftar-link'){
  const now=Date.now(),old=previous&&typeof previous==='object'&&!Array.isArray(previous)&&Object.prototype.hasOwnProperty.call(previous,'value')?previous:{};
  return {...old,value:JSON.stringify(next),valueEncoding:'local-storage-json-v1',deleted:false,updatedAt:now,revision:num(old.revision)+1,deviceId:'daftar-v17',page};
}
async function announceDataset(tenant,logical,groupId){
  const path=metaPath(tenant),key=stampKey(logical,groupId);
  for(let i=0;i<6;i++){
    const row=await readPath(path),old=actualObject(row.value,{}),at=Date.now();
    const next={...old,tenantId:tenant,companyId:tenant,datasetStampSchema:1,datasetStamps:{...(old.datasetStamps||{}),[key]:at},changedKeys:[...new Set([...(Array.isArray(old.changedKeys)?old.changedKeys:[]),key])].slice(-80),updatedAt:at};
    const r=await CashTopDirectAPI.rtdbCompareAndSet(path,next,row.updatedAt||0);if(r.ok)return true;
  }
  return false;
}
async function mutateDataset(tenant,logical,groupId,fallback,mutator){
  const path=datasetPath(tenant,logical,groupId);
  for(let i=0;i<7;i++){
    const row=await readPath(path),base=decodeStored(row.value,fallback),draft=clone(base==null?fallback:base),changed=await mutator(draft);
    if(changed===false)return {changed:false,data:draft};
    const wrapped=makeWrapper(row.value,draft);
    const w=await CashTopDirectAPI.rtdbCompareAndSet(path,wrapped,row.updatedAt||0);
    if(w.ok){await announceDataset(tenant,logical,groupId).catch(()=>{});return {changed:true,data:draft}}
  }
  throw new Error('تعارض أثناء تحديث بيانات كاش توب. أعد المحاولة.');
}

function companyFromAdmin(root,key){
  const wanted=normKey(key),state=actualObject(root,{}),seg=safeSeg(wanted),idx=state.keyIndex?.[seg];
  if(idx){const tid=clean(idx.tenantId||idx.companyId||idx.id);const c=state.companies?.[tid]||Object.values(state.companies||{}).find(x=>clean(x?.tenantId||x?.companyId)===tid);if(c&&normKey(c.key||c.companyKey)===wanted)return c}
  return Object.values(state.companies||{}).find(c=>c&&c.deleted!==true&&String(c.status||'')!=='deleted'&&normKey(c.key||c.companyKey)===wanted)||null;
}
function activeAccount(x){if(!x||typeof x!=='object'||x.active===false||x.disabled===true)return false;return !['inactive','disabled','blocked','مجمد','معطل','موقوف'].includes(clean(x.status).toLowerCase())}
function resolveBranch(branches,ref){const list=normalizeArray(branches),s=clean(ref);if(!s||s.toUpperCase()==='MAIN')return list.find(x=>x?.isMain===true)||list.find(x=>String(x?.id).toUpperCase()==='MAIN')||list[0]||null;return list.find(x=>String(x?.id)===s)||list.find(x=>clean(x?.name)===s)||null}
async function activeFinancialGroup(tenant){
  const {data}=await readDataset(tenant,'cashtop_financial_groups',LEGACY_GROUP,[]).catch(()=>({data:[]}));
  const groups=normalizeArray(data);return clean(groups.find(g=>g.status==='active')?.id||groups[groups.length-1]?.id||LEGACY_GROUP)||LEGACY_GROUP;
}
async function authenticateCashTop(key,username,password){
  const adminRow=await readPath(ADMIN_ROOT),company=companyFromAdmin(adminRow.value,key);
  if(!company)throw new Error('مفتاح كاش توب غير صحيح');
  if(company.deleted===true||String(company.status||'active')!=='active')throw new Error('شركة كاش توب متوقفة أو محذوفة');
  if(company.endAt&&Date.now()>=Date.parse(company.endAt))throw new Error('اشتراك كاش توب منتهي');
  const tenant=clean(company.tenantId||company.companyId);if(!tenant)throw new Error('معرّف شركة كاش توب غير مكتمل');
  const groupId=await activeFinancialGroup(tenant);
  const [{data:access},{data:branches},{data:employees},{data:agents}]=await Promise.all([
    readDataset(tenant,'cashtop_company_access',LEGACY_GROUP,{}).catch(()=>({data:{}})),
    readDataset(tenant,'cashtop_branches',LEGACY_GROUP,[]).catch(()=>({data:[]})),
    readDataset(tenant,'cashtop_employees',LEGACY_GROUP,[]).catch(()=>({data:[]})),
    readDataset(tenant,'cashtop_sales_agents',groupId,[]).catch(()=>({data:[]}))
  ]);
  const uname=normUser(username),pass=String(password??'');let account=null;
  const manager=actualObject(access,{}).manager||{};
  if(manager&&normUser(manager.username)===uname)account={id:manager.id||`ADMIN_${tenant}`,username:manager.username,password:manager.password,displayName:manager.displayName||manager.name||manager.username,role:'admin',active:manager.active!==false,branchId:'MAIN'};
  if(!account&&normUser(company.managerUsername)===uname)account={id:`ADMIN_${tenant}`,username:company.managerUsername,password:company.managerPassword,displayName:'مدير الشركة',role:'admin',active:true,branchId:'MAIN'};
  if(!account){const b=normalizeArray(branches).find(x=>normUser(x.managerUsername)===uname);if(b)account={id:b.managerUserId||`BRM_${b.id}`,username:b.managerUsername,password:b.managerPassword,displayName:b.manager||b.managerUsername,role:'branch-admin',active:b.managerActive!==false&&activeAccount(b),branchId:b.isMain===true?'MAIN':b.id}}
  if(!account){const e=normalizeArray(employees).find(x=>normUser(x.username)===uname);if(e){const b=resolveBranch(branches,e.branchRecordId||e.branchId||e.dataBranchId||'MAIN');account={id:e.id,username:e.username,password:e.password,displayName:e.name||e.username,role:'employee',active:activeAccount(e)&&(!b||activeAccount(b)),branchId:b?.isMain===true?'MAIN':(b?.id||e.branchId||'MAIN')}}}
  if(!account){const a=normalizeArray(agents).find(x=>normUser(x.username)===uname);if(a){const b=resolveBranch(branches,a.branchRecordId||a.branchId||'MAIN');account={id:a.id,username:a.username,password:a.password,displayName:a.name||a.username,role:'representative',active:activeAccount(a)&&a.cashierAccess!==false&&(!b||activeAccount(b)),branchId:b?.isMain===true?'MAIN':(b?.id||a.branchId||'MAIN')}}}
  if(!account||String(account.password??'')!==pass)throw new Error('اسم المستخدم أو كلمة المرور غير صحيحة');
  if(account.active===false)throw new Error('حساب كاش توب أو الفرع موقوف');
  return {tenantId:tenant,companyKey:normKey(key),companyName:company.companyName||actualObject(access,{}).companyName||'كاش توب',username:account.username,displayName:account.displayName,role:account.role,branchId:account.branchId||'MAIN',financialGroupId:groupId,linkedAt:Date.now(),baselinePending:true};
}

function supplierBalance(s){let bal=0;for(const m of normalizeArray(s?.movements)){if(m?.refundCash===true||num(m?.balanceEffect)===0&&m?.balanceEffect!==undefined)continue;const a=num(m.amount);if(['payment','return'].includes(String(m.type)))bal-=a;else bal+=a}return bal}
function customerBalance(c){return num(c?.balance)}
function findLocalByRemoteId(type,id){for(const [k,c] of Object.entries(contacts||{})){if(k.startsWith(type+':')&&String(c?.cashTop2Id||'')===String(id))return {key:k,name:k.slice(type.length+1),contact:c}}return null}
function findLocalByName(type,name){const key=personKey(name,type);return contacts[key]||records.some(r=>r.type===type&&r.name===name)?{key,name,contact:contacts[key]||{}}:null}
function ensureLocalContact(type,remote,{createOpening=false,balance=0,source='كاش توب'}={}){
  const rid=String(remote.id),remoteName=clean(remote.name)||`${type==='customers'?'عميل':'مورد'} ${rid.slice(-5)}`;
  let found=findLocalByRemoteId(type,rid)||findLocalByName(type,remoteName),name=found?.name||remoteName,oldKey=found?.key||'';
  if(found&&name!==remoteName&&found.contact?.cashTop2Id){records.forEach(r=>{if(r.type===type&&r.name===name)r.name=remoteName});const newKey=personKey(remoteName,type);contacts[newKey]={...(contacts[oldKey]||{}),cashTop2Id:rid};if(oldKey!==newKey)delete contacts[oldKey];name=remoteName;oldKey=newKey}
  const key=personKey(name,type);contacts[key]={...(contacts[key]||{}),phone:clean(remote.phone||contacts[key]?.phone).replace(/\D/g,''),cashTop2Id:rid,cashTop2Source:true,lastCashTop2Name:remoteName};
  if(!records.some(r=>r.type===type&&r.name===name))records.push({id:recordId('CT2_CONTACT',`${type}:${rid}`),type,name,note:'إضافة الحساب من كاش توب',debit:0,credit:0,date:'اليوم',time:shortTime(Date.now()),timestamp:Date.now(),externalSource:'cashtop2',externalRef:`ct2:contact:${type}:${rid}`});
  if(createOpening&&Math.abs(balance)>.0001){const ts=Date.now();records.push({id:recordId('CT2_OPEN',`${type}:${rid}`),type,name,note:`رصيد افتتاحي عند ربط ${source}`,debit:balance<0?Math.abs(balance):0,credit:balance>0?balance:0,date:'اليوم',time:shortTime(ts),timestamp:ts,externalSource:'cashtop2',externalRef:`ct2:opening:${type}:${rid}`});appendLedger({id:`ledger:ct2:opening:${type}:${rid}`,timestamp:ts,account:name,description:`رصيد افتتاحي عند ربط ${source}`,document:'ربط',debit:balance<0?Math.abs(balance):0,credit:balance>0?balance:0,source:'كاش توب',origin:'cashtop2',entityType:'party',partyType:type,partyName:name,externalRef:`ct2:opening:${type}:${rid}`})}
  return {key,name,contact:contacts[key]};
}

async function appendLedger(entry){
  if(typeof ledgerRecords==='undefined')return null;const e={id:entry.id||recordId('LED',entry.externalRef||crypto.randomUUID()),timestamp:dateTs(entry.timestamp||Date.now()),account:entry.account||'',description:entry.description||'',document:entry.document||'',debit:num(entry.debit),credit:num(entry.credit),source:entry.source||'الدفتر',origin:entry.origin||'daftar',entityType:entry.entityType||'general',partyType:entry.partyType||'',partyName:entry.partyName||'',localRecordId:entry.localRecordId||'',externalRef:entry.externalRef||'',cashTop2SourceType:entry.cashTop2SourceType||'',cashTop2SourceId:entry.cashTop2SourceId||'',cashTop2FundLogId:entry.cashTop2FundLogId||'',detailType:entry.detailType||'',detailData:entry.detailData?clone(entry.detailData):null,createdAt:entry.createdAt||nowIso()};
  const i=ledgerRecords.findIndex(x=>String(x.id)===String(e.id)||(e.externalRef&&x.externalRef===e.externalRef));if(i>=0){const old=ledgerRecords[i],merged={...old,...e,detailData:e.detailData||old.detailData||null};ledgerRecords[i]=merged;saveData();try{await CashTopSync?.putEntity?.('ledger_record',merged.id,merged,{sortTs:merged.timestamp,queue:true})}catch(_){}return merged}ledgerRecords.push(e);saveData();try{await CashTopSync?.putEntity?.('ledger_record',e.id,e,{sortTs:e.timestamp,queue:true})}catch(_){}return e;
}
async function registerLocalTransaction(r,source){
  if(!r)return;const isCash=source==='cash'||r.type==='in'||r.type==='out';
  if(isCash){await appendLedger({id:`ledger:local:cash:${r.id}`,timestamp:r.timestamp,account:'دفتر النقدية',description:r.note||(r.type==='in'?'دخل':'مصروف'),document:'الدفتر',debit:r.type==='out'?num(r.amount):0,credit:r.type==='in'?num(r.amount):0,source:'الدفتر',origin:'daftar',entityType:'cash',localRecordId:String(r.id),externalRef:`local:cash:${r.id}`});return}
  await appendLedger({id:`ledger:local:party:${r.id}`,timestamp:r.timestamp,account:r.name,description:r.note||'حركة حساب',document:r.type==='customers'?'عميل':'مورد',debit:num(r.debit),credit:num(r.credit),source:'الدفتر',origin:'daftar',entityType:'party',partyType:r.type,partyName:r.name,localRecordId:String(r.id),externalRef:`local:party:${r.id}`});
  scheduleSync(100);
}

async function ensureRemoteContact(linkInfo,type,name){
  const key=personKey(name,type),local=contacts[key]||{},logical=type==='customers'?'cashtop_customers':'cashtop_suppliers';let remoteId=clean(local.cashTop2Id),result=null;
  const groupId=linkInfo.financialGroupId;
  await mutateDataset(linkInfo.tenantId,logical,groupId,[],arr=>{
    let list=normalizeArray(arr);if(!Array.isArray(arr)){for(const k of Object.keys(arr))delete arr[k];list.forEach((x,i)=>arr[i]=x)}
    let idx=remoteId?list.findIndex(x=>String(x?.id)===remoteId):-1;
    if(idx<0)idx=list.findIndex(x=>clean(x?.name)===name&&(clean(local.phone)?clean(x?.phone).replace(/\D/g,'')===clean(local.phone).replace(/\D/g,''):true));
    if(idx>=0){const x=list[idx];remoteId=String(x.id);let changed=false;if(clean(x.name)!==name){x.name=name;changed=true}const p=clean(local.phone).replace(/\D/g,'');if(p&&clean(x.phone).replace(/\D/g,'')!==p){x.phone=p;changed=true}if(changed)x.updatedAt=nowIso();result=x;return changed}
    remoteId=`DF_${type==='customers'?'CUST':'SUP'}_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
    const x=type==='customers'?{id:remoteId,name,phone:clean(local.phone).replace(/\D/g,''),balance:0,debtInvoices:[],active:true,source:'daftar-link',createdAt:nowIso(),updatedAt:nowIso()}:{id:remoteId,name,phone:clean(local.phone).replace(/\D/g,''),active:true,movements:[],source:'daftar-link',createdAt:nowIso(),updatedAt:nowIso()};
    if(Array.isArray(arr))arr.push(x);else arr[Object.keys(arr).length]=x;result=x;return true;
  });
  contacts[key]={...local,cashTop2Id:remoteId,cashTop2Source:local.cashTop2Source||false};saveData();try{await CashTopSync.putEntity('contact',key,contacts[key],{queue:true})}catch(_){}return result;
}

async function pushLocalPartyRecord(linkInfo,r,s){
  if(!r||s.pushedLocal[String(r.id)]||r.externalSource==='cashtop2')return false;
  const amountCredit=num(r.credit),amountDebit=num(r.debit);if(!(amountCredit>0||amountDebit>0))return false;
  const type=r.type;if(!['customers','suppliers'].includes(type))return false;
  const remote=await ensureRemoteContact(linkInfo,type,r.name),rid=String(remote.id),delta=amountCredit-amountDebit,ref=`DF_${String(r.id)}`;
  if(type==='customers'){
    await mutateDataset(linkInfo.tenantId,'cashtop_customers',linkInfo.financialGroupId,[],arr=>{
      const list=normalizeArray(arr),c=list.find(x=>String(x?.id)===rid);if(!c)throw new Error('العميل غير موجود في كاش توب');c._daftarLinkRefs=Array.isArray(c._daftarLinkRefs)?c._daftarLinkRefs:[];if(c._daftarLinkRefs.includes(ref))return false;c.balance=num(c.balance)+delta;c._daftarLinkRefs.push(ref);if(c._daftarLinkRefs.length>3000)c._daftarLinkRefs=c._daftarLinkRefs.slice(-3000);
      if(amountCredit>0){c.debtInvoices=Array.isArray(c.debtInvoices)?c.debtInvoices:[];c.debtInvoices.push({id:`DF_DEBT_${r.id}`,type:'manual-debt',amount:amountCredit,remaining:amountCredit,date:new Date(r.timestamp||Date.now()).toISOString().slice(0,10),reference:`DF-${r.id}`,notes:r.note||'حركة من دفتر كاش توب',source:'daftar-link',sourceId:String(r.id),createdAt:nowIso(),updatedAt:nowIso()})}c.updatedAt=nowIso();return true;
    });
    if(amountDebit>0){await mutateDataset(linkInfo.tenantId,'cashtop_vouchers',linkInfo.financialGroupId,[],arr=>{const list=normalizeArray(arr);if(list.some(x=>String(x?.sourceId||'')===String(r.id)&&x?.source==='daftar-link'))return false;const v={id:`DF_VOU_${r.id}`,refNumber:`#DF-${r.id}`,type:'قبض',date:new Date(r.timestamp||Date.now()).toISOString().slice(0,10),relationType:'client',relationId:rid,relationName:r.name,amount:amountDebit,notes:r.note||'دفعة مسجلة من الدفتر',source:'daftar-link',sourceId:String(r.id),nonCash:true,createdAt:nowIso(),updatedAt:nowIso()};if(Array.isArray(arr))arr.push(v);else arr[Object.keys(arr).length]=v;return true})}
    s.observedBalances[`customers:${rid}`]=customerBalance({...remote,balance:num(remote.balance)+delta});
  }else{
    await mutateDataset(linkInfo.tenantId,'cashtop_suppliers',linkInfo.financialGroupId,[],arr=>{const list=normalizeArray(arr),sp=list.find(x=>String(x?.id)===rid);if(!sp)throw new Error('المورد غير موجود في كاش توب');sp.movements=Array.isArray(sp.movements)?sp.movements:[];if(sp.movements.some(m=>String(m?.sourceId||m?.refId||'')===String(r.id)&&m?.source==='daftar-link'))return false;sp.movements.push({id:`DF_SUP_${r.id}`,type:amountCredit>0?'debt':'payment',amount:Math.abs(delta),note:r.note||'حركة من دفتر كاش توب',date:new Date(r.timestamp||Date.now()).toISOString().slice(0,10),refType:'daftar-link',refId:String(r.id),source:'daftar-link',sourceId:String(r.id),createdAt:nowIso()});sp.updatedAt=nowIso();return true});
    s.observedBalances[`suppliers:${rid}`]=supplierBalance({...remote,movements:[...(remote.movements||[]),{type:amountCredit>0?'debt':'payment',amount:Math.abs(delta)}]});
  }
  s.pushedLocal[String(r.id)]={at:Date.now(),remoteId:rid};r.cashTop2Pushed=true;r.cashTop2Id=rid;saveData();return true;
}

async function syncLocalContactsAndRecords(linkInfo,s){
  const linkedAt=num(linkInfo.linkedAt);
  for(const k of Object.keys(contacts||{})){
    const m=k.match(/^(customers|suppliers):(.*)$/);if(!m)continue;await ensureRemoteContact(linkInfo,m[1],m[2]);
  }
  // لا نرسل أي حركة قديمة سبقت لحظة الربط. هذا يمنع مضاعفة الأرصدة عند أول ربط.
  const rows=(records||[]).filter(r=>['customers','suppliers'].includes(r.type)&&num(r.timestamp)>=linkedAt&&(num(r.credit)>0||num(r.debit)>0));
  for(const r of rows)await pushLocalPartyRecord(linkInfo,r,s);

  // إذا أُضيفت حركة في الدفتر ثم أُرشفت قبل وصول دورة المزامنة، تبقى نسختها في دفتر الأستاذ.
  // نستخدمها كطابور احتياطي حتى لا تضيع الحركة من كاش توب، بدون إعادة إرسال الحركات القديمة المزروعة قبل الربط.
  if(typeof ledgerRecords!=='undefined'&&Array.isArray(ledgerRecords)){
    const pending=ledgerRecords.filter(e=>e&&e.entityType==='party'&&String(e.origin||'').startsWith('daftar')&&num(e.timestamp)>=linkedAt&&e.localRecordId&&!s.pushedLocal[String(e.localRecordId)]);
    for(const e of pending){
      const pseudo={id:String(e.localRecordId),type:e.partyType,name:e.partyName,note:e.description||'حركة من الدفتر',debit:num(e.debit),credit:num(e.credit),timestamp:num(e.timestamp),externalSource:''};
      await pushLocalPartyRecord(linkInfo,pseudo,s);
    }
  }
}

function latestPartyHint(type,remote,vouchers){
  if(type==='customers'){
    const d=normalizeArray(remote.debtInvoices).slice().sort((a,b)=>dateTs(b.updatedAt||b.createdAt||b.date)-dateTs(a.updatedAt||a.createdAt||a.date))[0];
    const v=normalizeArray(vouchers).filter(x=>String(x.relationId)===String(remote.id)&&['client','customer'].includes(String(x.relationType||'').toLowerCase())).sort((a,b)=>dateTs(b.updatedAt||b.createdAt||b.date)-dateTs(a.updatedAt||a.createdAt||a.date))[0];
    const newest=[d&&{t:dateTs(d.updatedAt||d.createdAt||d.date),text:d.notes||d.reference||'دين/مبيعات من كاش توب'},v&&{t:dateTs(v.updatedAt||v.createdAt||v.date),text:v.notes||`${v.type||'سند'} من كاش توب`}].filter(Boolean).sort((a,b)=>b.t-a.t)[0];return newest?.text||'تغير رصيد العميل في كاش توب';
  }
  const m=normalizeArray(remote.movements).slice().sort((a,b)=>dateTs(b.createdAt||b.updatedAt||b.date)-dateTs(a.createdAt||a.updatedAt||a.date))[0];return m?.note||m?.productNames||'تغير رصيد المورد في كاش توب';
}
async function applyRemotePartyDelta(type,remote,delta,hint){
  const info=ensureLocalContact(type,remote,{createOpening:false}),ts=Date.now(),isIncrease=delta>0,amount=Math.abs(delta),ref=`ct2:balance:${type}:${remote.id}:${ts}:${Math.round(delta*1000)}`;
  const r={id:recordId('CT2_BAL',ref),type,name:info.name,note:hint||`حركة رصيد من كاش توب`,debit:isIncrease?0:amount,credit:isIncrease?amount:0,date:'اليوم',time:shortTime(ts),timestamp:ts,externalSource:'cashtop2',externalRef:ref,cashTop2Id:String(remote.id)};records.push(r);saveData();try{await CashTopSync.putEntity('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp,queue:true})}catch(_){}
  await appendLedger({id:`ledger:${ref}`,timestamp:ts,account:info.name,description:r.note,document:type==='customers'?'عميل':'مورد',debit:r.debit,credit:r.credit,source:'كاش توب',origin:'cashtop2',entityType:'party',partyType:type,partyName:info.name,externalRef:ref});
}

async function applyRemotePartyEvent(type,remote,effect,note,ref,ts,meta={}){
  const info=ensureLocalContact(type,remote,{createOpening:false}),amount=Math.abs(effect);if(!(amount>.0001))return 0;ts=dateTs(ts);
  const r={id:recordId('CT2_EVT',ref),type,name:info.name,note:note||'حركة من كاش توب',debit:effect<0?amount:0,credit:effect>0?amount:0,date:'اليوم',time:shortTime(ts),timestamp:ts,externalSource:'cashtop2',externalRef:ref,cashTop2Id:String(remote.id),...sourceMeta(meta.cashTop2SourceType,meta.cashTop2SourceId,{cashTop2FundLogId:meta.cashTop2FundLogId||'',detailType:meta.detailType||'',detailData:meta.detailData?clone(meta.detailData):null})};records.push(r);saveData();try{await CashTopSync.putEntity('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp,queue:true})}catch(_){}
  await appendLedger({id:`ledger:${ref}`,timestamp:ts,account:info.name,description:r.note,document:meta.document||(type==='customers'?'عميل':'مورد'),debit:r.debit,credit:r.credit,source:'كاش توب',origin:'cashtop2',entityType:'party',partyType:type,partyName:info.name,externalRef:ref,cashTop2SourceType:r.cashTop2SourceType,cashTop2SourceId:r.cashTop2SourceId,cashTop2FundLogId:r.cashTop2FundLogId,detailType:r.detailType,detailData:r.detailData});return effect;
}
async function syncCustomerEvents(c,vouchers,invoices,s,baseline){
  let effect=0;const cid=String(c.id),invoiceRows=normalizeArray(invoices);
  for(const d of normalizeArray(c.debtInvoices)){
    const id=String(d?.id||d?.invoiceId||`${d?.date||''}:${d?.amount||d?.remaining||0}:${d?.reference||''}`),ref=`ct2:cdebt:${cid}:${id}`;if(s.seenParty[ref])continue;s.seenParty[ref]=Date.now();if(d?.source==='daftar-link')continue;const amount=Math.max(0,num(d?.amount||d?.remaining||d?.debt));if(baseline||!(amount>0))continue;
    const invoiceId=clean(d?.invoiceId||((String(d?.sourceType||'')==='sale'&&String(id).startsWith('INV_'))?id:'')),inv=invoiceId?rowById(invoiceRows,invoiceId):null;if(invoiceId&&isTombstoned(s,'sale',invoiceId))continue;
    const meta=invoiceId?{cashTop2SourceType:'sale',cashTop2SourceId:invoiceId,detailType:'invoice',detailData:invoiceSnapshot(inv),document:'فاتورة مبيعات'}:{cashTop2SourceType:'customerDebt',cashTop2SourceId:id};
    effect+=await applyRemotePartyEvent('customers',c,amount,inv?'تحصيل فاتورة':(d?.notes||d?.reference||(d?.sourceType==='sale'?'تحصيل فاتورة':'دين عميل من كاش توب')),ref,inv?invoiceTimestamp(inv,d):dateTs(d?.updatedAt||d?.createdAt||d?.date),meta);
  }
  for(const v of normalizeArray(vouchers)){
    const rel=String(v?.relationType||'').toLowerCase();if(!['client','customer'].includes(rel)||String(v?.relationId||'')!==cid)continue;const id=String(v?.id||`${v?.date||''}:${v?.amount||0}:${v?.refNumber||''}`),ref=`ct2:cvoucher:${cid}:${id}`;if(s.seenParty[ref]||isTombstoned(s,'voucher',id))continue;s.seenParty[ref]=Date.now();if(v?.source==='daftar-link')continue;const amount=Math.max(0,num(v?.amount));if(baseline||!(amount>0))continue;const typ=String(v?.type||'').toLowerCase(),sign=(typ.includes('صرف')||typ.includes('debit'))?1:-1;effect+=await applyRemotePartyEvent('customers',c,sign*amount,v?.notes||`${v?.type||'سند'} عميل من كاش توب`,ref,dateTs(v?.updatedAt||v?.createdAt||v?.date),{cashTop2SourceType:'voucher',cashTop2SourceId:id,detailType:'voucher',detailData:clone(v),document:'سند'});
  }
  return effect;
}
async function syncSupplierEvents(sp,s,baseline){
  let effect=0;const sid=String(sp.id);
  for(const m of normalizeArray(sp.movements)){
    const id=String(m?.id||`${m?.date||''}:${m?.type||''}:${m?.amount||0}:${m?.refId||''}`),ref=`ct2:smove:${sid}:${id}`;if(s.seenParty[ref]||isTombstoned(s,'supplierMovement',id))continue;s.seenParty[ref]=Date.now();if(m?.source==='daftar-link'||m?.refType==='daftar-link')continue;if(m?.refundCash===true||num(m?.balanceEffect)===0&&m?.balanceEffect!==undefined)continue;const amount=Math.max(0,num(m?.amount));if(baseline||!(amount>0))continue;const sign=['payment','return'].includes(String(m?.type))?-1:1;effect+=await applyRemotePartyEvent('suppliers',sp,sign*amount,m?.note||m?.productNames||(sign<0?'دفعة/مرتجع مورد من كاش توب':'مشتريات/دين مورد من كاش توب'),ref,dateTs(m?.updatedAt||m?.createdAt||m?.date),{cashTop2SourceType:'supplierMovement',cashTop2SourceId:id,detailType:'supplierMovement',detailData:clone(m),document:'حركة مورد'});
  }
  return effect;
}

function cashLogAmount(log){const b=num(log?.baseAmount);return b>0?b:num(log?.amount)}
function isIncomingLog(log){const t=clean(log?.type).toLowerCase();return t.includes('إيداع')||t.includes('deposit')||t.includes('دخل')||t.includes('قبض')||t.includes('إضافة')}
function rowById(rows,id){return normalizeArray(rows).find(x=>String(x?.id)===String(id))||null}
function partyFromRelation(type,id,name,ctx){
  const rel=clean(type).toLowerCase();if(!['client','customer','supplier'].includes(rel))return null;
  const partyType=rel==='supplier'?'suppliers':'customers',list=partyType==='suppliers'?ctx.suppliers:ctx.customers,row=rowById(list,id);
  return {type:partyType,id:clean(id||row?.id),name:clean(name||row?.name)||(partyType==='suppliers'?'مورد':'عميل')};
}
function partyCashContext(log,ctx){
  const direct=partyFromRelation(log?.relationType,log?.relationId,log?.relationName,ctx);if(direct)return direct;
  const src=clean(log?.sourceType),sid=clean(log?.sourceId);
  if(src==='voucher'){
    const v=rowById(ctx.vouchers,sid),p=partyFromRelation(v?.relationType,v?.relationId,v?.relationName,ctx);if(p)return p;
  }
  if(['sale','sale-reversal'].includes(src)){
    let inv=rowById(ctx.invoices,sid);
    if(!inv&&src==='sale-reversal'){
      // عند حذف/تعديل الفاتورة قد يختفي السجل الأصلي، فنستفيد من حركة البيع الأصلية ذات نفس المرجع.
      const sibling=normalizeArray(ctx.fundLogs).find(x=>String(x?.sourceId||'')===sid&&String(x?.sourceType||'')==='sale');
      if(sibling){const m=clean(sibling.notes).match(/\[([^\]]+)\]\s*$/);if(m&&m[1]&&m[1]!=='عميل نقدي')return {type:'customers',id:'',name:m[1]}}
    }
    const name=clean(inv?.customerName||inv?.customer),cid=clean(inv?.customerId);if((cid||name)&&name!=='عميل نقدي')return {type:'customers',id:cid,name:name||clean(rowById(ctx.customers,cid)?.name)||'عميل'};
  }
  if(['salesReturn','salesReturn-reversal'].includes(src)){
    const r=rowById(ctx.salesReturns,sid);if(r&&(r.customerId||r.customerName))return {type:'customers',id:clean(r.customerId),name:clean(r.customerName)||clean(rowById(ctx.customers,r.customerId)?.name)||'عميل'};
    const sibling=normalizeArray(ctx.fundLogs).find(x=>String(x?.sourceId||'')===sid&&String(x?.sourceType||'')==='salesReturn'),note=clean(sibling?.notes||log?.notes),m=note.match(/للعميل\s+(.+?)(?:\s*\||$)/);if(m?.[1])return {type:'customers',id:'',name:clean(m[1])};
  }
  if(['purchaseInvoice','purchaseInvoiceReversal','purchaseItemReversal'].includes(src)){
    const r=rowById(ctx.purchases,sid);if(r&&r.withoutSupplier!==true&&(r.supplierId||r.supplierName))return {type:'suppliers',id:clean(r.supplierId),name:clean(r.supplierName)||clean(rowById(ctx.suppliers,r.supplierId)?.name)||'مورد'};
  }
  if(['purchaseReturn','purchaseReturn-reversal'].includes(src)){
    const r=rowById(ctx.purchaseReturns,sid);if(r&&(r.supplierId||r.supplierName))return {type:'suppliers',id:clean(r.supplierId),name:clean(r.supplierName)||clean(rowById(ctx.suppliers,r.supplierId)?.name)||'مورد'};
  }
  if(src==='materialPurchase'){
    const r=rowById(ctx.materialPurchases,sid);if(r&&(r.supplierId||r.supplierName))return {type:'suppliers',id:clean(r.supplierId),name:clean(r.supplierName)||clean(rowById(ctx.suppliers,r.supplierId)?.name)||'مورد'};
  }
  if(src==='supplierPayment'){
    for(const sp of normalizeArray(ctx.suppliers)){const m=normalizeArray(sp.movements).find(x=>String(x?.id||x?.refId||'')===sid||String(x?.refId||'')===sid);if(m)return {type:'suppliers',id:clean(sp.id),name:clean(sp.name)||'مورد'}}
    // هذه الحركة مورد حتى لو تعذر حل الاسم؛ لا نرسلها إلى دفتر النقدية العام كي لا تتكرر كحركة عامة.
    return {type:'suppliers',id:'',name:'مورد'};
  }
  // بعض نسخ كاش توب القديمة لم تكن تضع relationType/sourceType في كل عكس قيد.
  // إذا كان البيان نفسه يصرّح بعميل أو مورد، نربطه بالاسم بدلاً من اعتباره دخل/مصروف عام.
  const note=clean(log?.notes);
  if(/مورد|للمورد/.test(note)){const sp=normalizeArray(ctx.suppliers).find(x=>clean(x?.name)&&note.includes(clean(x.name)));if(sp)return {type:'suppliers',id:clean(sp.id),name:clean(sp.name)}}
  if(/عميل|للعميل/.test(note)){const c=normalizeArray(ctx.customers).find(x=>clean(x?.name)&&note.includes(clean(x.name)));if(c)return {type:'customers',id:clean(c.id),name:clean(c.name)}}
  return null;
}
async function importCashLog(log,ctx,s,baseline){
  const logId=String(log.id||`${log.sourceType}:${log.sourceId}:${log.date}:${log.amount}`),ref=`ct2:fund:${logId}`;if(s.seenCash[ref])return;
  const amount=cashLogAmount(log);if(!(amount>0)){s.seenCash[ref]=Date.now();return}
  const src=clean(log?.sourceType),sourceId=clean(log?.sourceId),invoice=src==='sale'&&sourceId?rowById(ctx.invoices,sourceId):null;
  if((src==='sale'||src==='sale-reversal')&&sourceId&&isTombstoned(s,'sale',sourceId)){s.seenCash[ref]=Date.now();return}
  // إذا حذف المستخدم المصدر من الدفتر، لا نسمح لأي قيد صندوق جديد لنفس المصدر أن يعيده لاحقاً.
  if(src&&sourceId&&isTombstoned(s,src,sourceId)){s.seenCash[ref]=Date.now();return}
  if(isTombstoned(s,'fundLog',logId)){s.seenCash[ref]=Date.now();return}
  const incoming=isIncomingLog(log),ts=invoice?invoiceTimestamp(invoice,log):dateTs(log.createdAt||log.updatedAt||log.date),party=partyCashContext(log,ctx);
  const isSale=src==='sale',customerName=isSale?invoiceCustomerName(invoice,log):(party?.name||''),description=isSale?'تحصيل فاتورة':(clean(log.notes)||src||'حركة صندوق من كاش توب');
  const meta={cashTop2SourceType:isSale?'sale':(src||'fundLog'),cashTop2SourceId:isSale?clean(invoice?.id||sourceId):sourceId,cashTop2FundLogId:logId,detailType:isSale?'invoice':'cashLog',detailData:isSale?invoiceSnapshot(invoice):clone(log)};
  // فاتورة البيع لها قاعدة مستقلة: الجزء المقبوض فقط يدخل دفتر النقدية،
  // حتى لو كانت الفاتورة مرتبطة بعميل. أما الدين فيدخل حساب العميل من debtInvoices.
  // بهذه الطريقة: جزئي = (مدفوع -> نقدية) + (متبقي -> عميل)، آجل = عميل فقط، كامل = نقدية فقط.
  const shouldWriteCash=!baseline&&(!party||isSale);
  if(shouldWriteCash){
    const exists=(cashRecords||[]).some(x=>x&&x.externalRef===ref);
    if(!exists){
      const r={id:recordId('CT2_CASH',ref),type:incoming?'in':'out',amount,note:description,catName:isSale?(customerName||'عميل نقدي'):'كاش توب',catIcon:'fa-link',image:'',imageUrl:'',date:'اليوم',time:shortTime(ts),timestamp:ts,externalSource:'cashtop2',externalRef:ref,...sourceMeta(meta.cashTop2SourceType,meta.cashTop2SourceId,{cashTop2FundLogId:logId,detailType:meta.detailType,detailData:meta.detailData})};cashRecords.push(r);saveData();try{await CashTopSync.putEntity('cash_record',r.id,r,{sortTs:r.timestamp,queue:true})}catch(_){}
    }
  }
  if(!baseline)await appendLedger({id:`ledger:${ref}`,timestamp:ts,account:isSale?'دفتر النقدية':(party?.name||clean(log.accountName)||clean(log.accountId)||'صندوق كاش توب'),description,document:isSale?'فاتورة مبيعات':(src||clean(log.refType)||'صندوق'),debit:incoming?0:amount,credit:incoming?amount:0,source:'كاش توب',origin:'cashtop2',entityType:isSale?'cash':(party?'party-cash':'cash'),partyType:party?.type||'',partyName:isSale?(customerName||party?.name||''):party?.name||'',externalRef:ref,...meta});
  s.seenCash[ref]=Date.now();
}

function matchesExternal(row,kind,id){if(!row)return false;const sid=clean(row.cashTop2SourceId),st=clean(row.cashTop2SourceType),wanted=String(id);if(st===kind&&sid===wanted)return true;if(kind==='sale'&&row.detailType==='invoice'&&clean(row.detailData?.id)===wanted)return true;if(kind==='sale'){const text=`${row.note||''} ${row.description||''} ${row.externalRef||''}`;if(text.includes(`[${wanted}]`)||text.includes(wanted))return true}return false}
async function upgradeLegacyInvoiceRecords(logs,invoices){const logRows=normalizeArray(logs),invRows=normalizeArray(invoices);let changed=false;const findByRef=r=>{const ref=clean(r?.externalRef);if(!ref.startsWith('ct2:fund:'))return null;const lid=ref.slice('ct2:fund:'.length);return logRows.find(x=>String(x?.id)===lid)||null};const upgrade=async(r,kind)=>{if(!r||r.externalSource!=='cashtop2'||r.detailType==='invoice')return;const log=findByRef(r);if(!log||String(log.sourceType)!=='sale')return;const inv=rowById(invRows,log.sourceId);if(!inv)return;const ts=invoiceTimestamp(inv,log),customer=invoiceCustomerName(inv,log);r.note='تحصيل فاتورة';if(kind==='cash')r.catName=customer;r.timestamp=ts;r.date='اليوم';r.time=shortTime(ts);Object.assign(r,sourceMeta('sale',inv.id,{cashTop2FundLogId:String(log.id||''),detailType:'invoice',detailData:invoiceSnapshot(inv)}));changed=true;try{if(kind==='cash')await CashTopSync?.putEntity?.('cash_record',r.id,r,{sortTs:ts,queue:true});else await CashTopSync?.putEntity?.('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:ts,queue:true})}catch(_){}};for(const r of cashRecords||[])await upgrade(r,'cash');for(const r of records||[])await upgrade(r,'debt');if(typeof ledgerRecords!=='undefined'&&Array.isArray(ledgerRecords)){for(let i=0;i<ledgerRecords.length;i++){const e=ledgerRecords[i];if(e.detailType==='invoice')continue;const log=findByRef(e);if(!log||String(log.sourceType)!=='sale')continue;const inv=rowById(invRows,log.sourceId);if(!inv)continue;const ts=invoiceTimestamp(inv,log),customer=invoiceCustomerName(inv,log),next={...e,timestamp:ts,account:customer,description:'تحصيل فاتورة',document:'فاتورة مبيعات',cashTop2SourceType:'sale',cashTop2SourceId:String(inv.id),cashTop2FundLogId:String(log.id||''),detailType:'invoice',detailData:invoiceSnapshot(inv)};ledgerRecords[i]=next;changed=true;try{await CashTopSync?.putEntity?.('ledger_record',next.id,next,{sortTs:ts,queue:true})}catch(_){}}}if(changed)saveData();return changed}


async function upgradeV17InvoiceSplit(logs,invoices,s,baseline){
  const logRows=normalizeArray(logs),invRows=normalizeArray(invoices);let changed=false;
  for(const log of logRows){
    if(String(log?.sourceType||'')!=='sale')continue;
    const logId=String(log.id||`${log.sourceType}:${log.sourceId}:${log.date}:${log.amount}`),ref=`ct2:fund:${logId}`,sourceId=clean(log?.sourceId);
    if(!sourceId||isTombstoned(s,'sale',sourceId)||isTombstoned(s,'fundLog',logId))continue;
    const amount=cashLogAmount(log);if(!(amount>0))continue;
    const inv=rowById(invRows,sourceId);if(!inv)continue;
    // في أول ربط لا نستورد التاريخ القديم. الترقية تخص فقط حركة سبق أن عرفتها نسخة v16 محلياً.
    const known=!!s.seenCash?.[ref]||(cashRecords||[]).some(x=>x?.externalRef===ref)||(typeof ledgerRecords!=='undefined'&&Array.isArray(ledgerRecords)&&ledgerRecords.some(x=>x?.externalRef===ref));
    if(baseline&&!known)continue;
    if(!known)continue;
    const ts=invoiceTimestamp(inv,log),customerName=invoiceCustomerName(inv,log),snapshot=invoiceSnapshot(inv);
    let cash=(cashRecords||[]).find(x=>x?.externalRef===ref);
    if(!cash){
      cash={id:recordId('CT2_CASH',ref),type:isIncomingLog(log)?'in':'out',amount,note:'تحصيل فاتورة',catName:customerName||'عميل نقدي',catIcon:'fa-link',image:'',imageUrl:'',date:'اليوم',time:shortTime(ts),timestamp:ts,externalSource:'cashtop2',externalRef:ref,...sourceMeta('sale',sourceId,{cashTop2FundLogId:logId,detailType:'invoice',detailData:snapshot})};
      cashRecords.push(cash);changed=true;try{await CashTopSync?.putEntity?.('cash_record',cash.id,cash,{sortTs:ts,queue:true})}catch(_){}
    }else{
      const next={...cash,type:isIncomingLog(log)?'in':'out',amount,note:'تحصيل فاتورة',catName:customerName||'عميل نقدي',timestamp:ts,date:'اليوم',time:shortTime(ts),...sourceMeta('sale',sourceId,{cashTop2FundLogId:logId,detailType:'invoice',detailData:snapshot})};
      Object.assign(cash,next);changed=true;try{await CashTopSync?.putEntity?.('cash_record',cash.id,cash,{sortTs:ts,queue:true})}catch(_){}
    }
    if(typeof ledgerRecords!=='undefined'&&Array.isArray(ledgerRecords)){
      const idx=ledgerRecords.findIndex(x=>x?.externalRef===ref);if(idx>=0){
        const old=ledgerRecords[idx],next={...old,timestamp:ts,account:'دفتر النقدية',description:'تحصيل فاتورة',document:'فاتورة مبيعات',debit:isIncomingLog(log)?0:amount,credit:isIncomingLog(log)?amount:0,source:'كاش توب',origin:'cashtop2',entityType:'cash',partyType:clean(inv?.customerId||inv?.customer)?'customers':'',partyName:customerName,cashTop2SourceType:'sale',cashTop2SourceId:sourceId,cashTop2FundLogId:logId,detailType:'invoice',detailData:snapshot};
        ledgerRecords[idx]=next;changed=true;try{await CashTopSync?.putEntity?.('ledger_record',next.id,next,{sortTs:ts,queue:true})}catch(_){}
      }
    }
  }
  if(changed)saveData();return changed;
}

async function removeLedgerRows(predicate){if(typeof ledgerRecords==='undefined'||!Array.isArray(ledgerRecords))return 0;const doomed=ledgerRecords.filter(predicate);if(!doomed.length)return 0;ledgerRecords=ledgerRecords.filter(x=>!doomed.includes(x));for(const e of doomed){try{await CashTopSync?.deleteEntity?.('ledger_record',e.id,{sortTs:e.timestamp||0})}catch(_){}}return doomed.length}
async function removeLocalExternal(kind,id){id=String(id||'');if(!id)return 0;const debt=(records||[]).filter(r=>matchesExternal(r,kind,id)),cash=(cashRecords||[]).filter(r=>matchesExternal(r,kind,id));for(const r of debt){try{await CashTopSync?.deleteEntityAndImage?.('debt_record',r.id,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0},r)}catch(_){}}for(const r of cash){try{await CashTopSync?.deleteEntityAndImage?.('cash_record',r.id,{sortTs:r.timestamp||0},r)}catch(_){}}records=(records||[]).filter(r=>!debt.includes(r));cashRecords=(cashRecords||[]).filter(r=>!cash.includes(r));await removeLedgerRows(e=>matchesExternal(e,kind,id));saveData();try{renderDebtsList?.();renderCashList?.();if(currentDetailName)renderPersonDetailList?.()}catch(_){}return debt.length+cash.length}
async function removeLocalFundLog(logId){logId=String(logId||'');if(!logId)return 0;const debt=(records||[]).filter(r=>String(r.cashTop2FundLogId||'')===logId),cash=(cashRecords||[]).filter(r=>String(r.cashTop2FundLogId||'')===logId);for(const r of debt){try{await CashTopSync?.deleteEntityAndImage?.('debt_record',r.id,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0},r)}catch(_){}}for(const r of cash){try{await CashTopSync?.deleteEntityAndImage?.('cash_record',r.id,{sortTs:r.timestamp||0},r)}catch(_){}}records=(records||[]).filter(r=>!debt.includes(r));cashRecords=(cashRecords||[]).filter(r=>!cash.includes(r));await removeLedgerRows(e=>String(e.cashTop2FundLogId||'')===logId||e.externalRef===`ct2:fund:${logId}`);saveData();return debt.length+cash.length}
function reverseFundLogInDb(db,log){db=actualObject(db,{accounts:[],accountLogs:[]});db.accounts=normalizeArray(db.accounts);db.accountLogs=normalizeArray(db.accountLogs);const amount=num(log?.amount),acc=db.accounts.find(a=>String(a?.id)===String(log?.accountId));if(acc&&amount){const incoming=isIncomingLog(log);acc.balance=num(acc.balance)+(incoming?-amount:amount)}db.accountLogs=db.accountLogs.filter(x=>String(x?.id)!==String(log?.id));return db}
function invoiceItemFactor(item){const direct=num(item?.factorToBase||item?.unitFactor||item?.selectedUnitFactor||item?.piecesPerUnit);if(direct>0)return direct;const uid=clean(item?.selectedUnitId||item?.unitId||item?.saleUnitId),chain=normalizeArray(item?.unitChain);const level=chain.find(x=>String(x?.id)===uid);return Math.max(.000001,num(level?.factorToBase)||1)}
async function deleteRemoteSalesInvoice(l,invoiceId,snapshot){invoiceId=String(invoiceId||'');if(!invoiceId)return true;let live=snapshot||null;
  await mutateDataset(l.tenantId,'cashtop_invoices',l.financialGroupId,[],arr=>{const list=normalizeArray(arr),idx=list.findIndex(x=>String(x?.id)===invoiceId);if(idx<0)return false;live=clone(list[idx]);if(Array.isArray(arr))arr.splice(idx,1);else{for(const k of Object.keys(arr))delete arr[k];list.filter((_,i)=>i!==idx).forEach((x,i)=>arr[i]=x)}return true});
  const inv=live||snapshot;if(!inv)return true;
  if(inv.customerId||inv.customer){await mutateDataset(l.tenantId,'cashtop_customers',l.financialGroupId,[],arr=>{const list=normalizeArray(arr),c=list.find(x=>String(x?.id)===String(inv.customerId))||list.find(x=>clean(x?.name)===clean(inv.customer));if(!c)return false;let changed=false;if(num(inv.debt)>0){c.balance=num(c.balance)-num(inv.debt);if(Math.abs(c.balance)<.000001)c.balance=0;changed=true}if(Array.isArray(c.debtInvoices)){const n=c.debtInvoices.length;c.debtInvoices=c.debtInvoices.filter(x=>String(x?.invoiceId||x?.id)!==invoiceId);changed=changed||c.debtInvoices.length!==n}if(changed)c.updatedAt=nowIso();return changed}).catch(()=>{})}
  await mutateDataset(l.tenantId,'cashtop_funds_db',l.financialGroupId,{accounts:[],accountLogs:[]},db=>{const obj=actualObject(db,{accounts:[],accountLogs:[]}),logs=normalizeArray(obj.accountLogs),doomed=logs.filter(x=>String(x?.sourceId||'')===invoiceId&&['sale','sale-reversal'].includes(String(x?.sourceType||'')));if(!doomed.length)return false;for(const log of doomed)reverseFundLogInDb(obj,log);db.accounts=obj.accounts;db.accountLogs=obj.accountLogs;return true}).catch(()=>{});
  await mutateDataset(l.tenantId,'cashtop_products',l.financialGroupId,[],arr=>{const list=normalizeArray(arr);let changed=false;for(const item of normalizeArray(inv.items)){if(!item||item.isCustom)continue;const p=list.find(x=>String(x?.id)===String(item.id??item.productId));if(!p||p.untrackedStock===true)continue;const qty=Math.max(0,num(item.qty||item.quantity)),pieces=qty*invoiceItemFactor(item);if(item.isVariant&&Array.isArray(p.variants)){const v=p.variants.find(x=>String(x?.size||'')===String(item.variantSize||'')&&String(x?.color||'')===String(item.variantColor||''));if(v)v.qty=Math.max(0,num(v.qty))+qty}p.stockPieces=Math.max(0,num(p.stockPieces))+pieces;changed=true}return changed}).catch(()=>{});
  await mutateDataset(l.tenantId,'cashtop_sales_reversals',l.financialGroupId,[],arr=>{const list=normalizeArray(arr);if(list.some(x=>String(x?.saleId)===invoiceId&&x?.source==='daftar-link'))return false;const row={id:`DF_SALE_REV_${invoiceId}_${Date.now()}`,saleId:invoiceId,branchId:inv.branchId||'MAIN',reversedAt:nowIso(),reason:'حذف من دفتر كاش توب',source:'daftar-link',originalInvoice:clone(inv)};if(Array.isArray(arr))arr.push(row);else arr[Object.keys(arr).length]=row;return true}).catch(()=>{});return true}
async function deleteRemoteVoucher(l,id,record){id=String(id||'');if(!id)return true;let removed=null;await mutateDataset(l.tenantId,'cashtop_vouchers',l.financialGroupId,[],arr=>{const list=normalizeArray(arr),idx=list.findIndex(x=>String(x?.id)===id);if(idx<0)return false;removed=clone(list[idx]);if(Array.isArray(arr))arr.splice(idx,1);else{for(const k of Object.keys(arr))delete arr[k];list.filter((_,i)=>i!==idx).forEach((x,i)=>arr[i]=x)}return true});const v=removed||record?.detailData;if(v&&['client','customer'].includes(String(v.relationType||'').toLowerCase()))await mutateDataset(l.tenantId,'cashtop_customers',l.financialGroupId,[],arr=>{const c=normalizeArray(arr).find(x=>String(x?.id)===String(v.relationId));if(!c)return false;const typ=String(v.type||'').toLowerCase(),sign=(typ.includes('صرف')||typ.includes('debit'))?1:-1;c.balance=num(c.balance)-sign*num(v.amount);c.updatedAt=nowIso();return true}).catch(()=>{});return true}
async function deleteRemoteSupplierMovement(l,id,record){id=String(id||'');if(!id)return true;const supplierId=clean(record?.cashTop2Id||record?.detailData?.supplierId);await mutateDataset(l.tenantId,'cashtop_suppliers',l.financialGroupId,[],arr=>{const list=normalizeArray(arr);let changed=false;for(const sp of list){if(supplierId&&String(sp.id)!==supplierId)continue;const before=normalizeArray(sp.movements),after=before.filter(m=>String(m?.id||'')!==id&&String(m?.refId||'')!==id);if(after.length!==before.length){sp.movements=after;sp.updatedAt=nowIso();changed=true;if(supplierId)break}}return changed});return true}
async function deleteRemoteFundLog(l,id){id=String(id||'');if(!id)return true;await mutateDataset(l.tenantId,'cashtop_funds_db',l.financialGroupId,{accounts:[],accountLogs:[]},db=>{const obj=actualObject(db,{accounts:[],accountLogs:[]}),log=normalizeArray(obj.accountLogs).find(x=>String(x?.id)===id);if(!log)return false;reverseFundLogInDb(obj,log);db.accounts=obj.accounts;db.accountLogs=obj.accountLogs;return true});return true}
async function deletePushedLocalParty(l,r){const type=r?.type;if(!['customers','suppliers'].includes(type))return true;const rid=clean(r.cashTop2Id||contacts?.[personKey(r.name,type)]?.cashTop2Id),id=String(r.id),delta=num(r.credit)-num(r.debit);if(!rid)return true;if(type==='customers'){await mutateDataset(l.tenantId,'cashtop_customers',l.financialGroupId,[],arr=>{const c=normalizeArray(arr).find(x=>String(x?.id)===rid);if(!c)return false;c.balance=num(c.balance)-delta;c._daftarLinkRefs=normalizeArray(c._daftarLinkRefs).filter(x=>String(x)!==`DF_${id}`);if(Array.isArray(c.debtInvoices))c.debtInvoices=c.debtInvoices.filter(x=>String(x?.sourceId||'')!==id&&String(x?.id||'')!==`DF_DEBT_${id}`);c.updatedAt=nowIso();return true});await mutateDataset(l.tenantId,'cashtop_vouchers',l.financialGroupId,[],arr=>{const list=normalizeArray(arr),next=list.filter(x=>!(x?.source==='daftar-link'&&String(x?.sourceId||'')===id));if(next.length===list.length)return false;if(Array.isArray(arr)){arr.splice(0,arr.length,...next)}else{for(const k of Object.keys(arr))delete arr[k];next.forEach((x,i)=>arr[i]=x)}return true}).catch(()=>{})}else await mutateDataset(l.tenantId,'cashtop_suppliers',l.financialGroupId,[],arr=>{const sp=normalizeArray(arr).find(x=>String(x?.id)===rid);if(!sp)return false;const before=normalizeArray(sp.movements),after=before.filter(m=>!(m?.source==='daftar-link'&&String(m?.sourceId||m?.refId||'')===id));if(after.length===before.length)return false;sp.movements=after;sp.updatedAt=nowIso();return true});return true}
async function executePendingDelete(l,key,p){if(!p)return true;const kind=p.kind,id=p.id,record=p.record||{};if(kind==='sale')return deleteRemoteSalesInvoice(l,id,record.detailData);if(kind==='voucher')return deleteRemoteVoucher(l,id,record);if(kind==='supplierMovement')return deleteRemoteSupplierMovement(l,id,record);if(kind==='fundLog')return deleteRemoteFundLog(l,id);if(kind==='localParty')return deletePushedLocalParty(l,record);return true}
async function processPendingDeletes(l,s){for(const [k,p] of Object.entries(s.pendingDeletes||{})){try{await executePendingDelete(l,k,p);delete s.pendingDeletes[k]}catch(e){console.warn('[CashTopLink] pending delete',k,e)}}}
async function handleLocalDelete(record){if(!record)return false;const l=link();if(!l)return false;const s=state();let kind='',id='',sourceKind='',sourceId='';if(record.externalSource==='cashtop2'){sourceKind=clean(record.cashTop2SourceType);sourceId=clean(record.cashTop2SourceId);if(sourceKind&&sourceId)markTombstone(s,sourceKind,sourceId,'daftar');kind=sourceKind;id=sourceId;const directlySupported=['sale','voucher','supplierMovement'];if(!directlySupported.includes(kind)&&record.cashTop2FundLogId){kind='fundLog';id=clean(record.cashTop2FundLogId)}if(!id&&record.cashTop2FundLogId){kind='fundLog';id=clean(record.cashTop2FundLogId)}if(record.cashTop2FundLogId)markTombstone(s,'fundLog',clean(record.cashTop2FundLogId),'daftar')}else if(record.cashTop2Pushed||s.pushedLocal?.[String(record.id)]){kind='localParty';id=String(record.id)}if(!kind||!id){await removeLedgerRows(e=>String(e.localRecordId||'')===String(record.id)||e.externalRef===record.externalRef);saveState(s);saveData();return false}const key=entityKey(kind,id);s.pendingDeletes[key]={kind,id,at:Date.now(),record:clone(record),sourceKind,sourceId};if(kind==='fundLog')markTombstone(s,'fundLog',id,'daftar');await removeLedgerRows(e=>String(e.localRecordId||'')===String(record.id)||(record.externalRef&&e.externalRef===record.externalRef)||(sourceKind&&sourceId&&matchesExternal(e,sourceKind,sourceId))||matchesExternal(e,kind,id));saveState(s);saveData();if(navigator.onLine&&!isExpired()){try{await executePendingDelete(l,key,s.pendingDeletes[key]);delete s.pendingDeletes[key];saveState(s)}catch(e){console.warn('[CashTopLink] delete remote',e)}}scheduleSync(250);return true}
function buildRemoteMap(rows,idFn,metaFn){const out={};for(const row of rows){const id=clean(idFn(row));if(id)out[id]={at:Date.now(),...(metaFn?metaFn(row):{})}}return out}
async function reconcileRemoteDeletions(s,{invoices,logs,vouchers,suppliers,baseline}){const affected=new Set(),currentInvoices=buildRemoteMap(normalizeArray(invoices),x=>x.id,x=>({customerId:clean(x.customerId),customer:clean(x.customer)})),currentLogs=buildRemoteMap(normalizeArray(logs),x=>x.id,x=>({sourceType:clean(x.sourceType),sourceId:clean(x.sourceId)})),currentVouchers=buildRemoteMap(normalizeArray(vouchers),x=>x.id,x=>({relationId:clean(x.relationId),relationType:clean(x.relationType)}));const movements=[];for(const sp of normalizeArray(suppliers))for(const m of normalizeArray(sp.movements))movements.push({...m,__supplierId:sp.id});const currentMoves=buildRemoteMap(movements,x=>x.id||x.refId,x=>({supplierId:clean(x.__supplierId)}));if(!baseline){for(const [id,meta] of Object.entries(s.remoteInvoices||{}))if(!currentInvoices[id]){markTombstone(s,'sale',id,'cashtop');await removeLocalExternal('sale',id);if(meta.customerId)affected.add(`customers:${meta.customerId}`)}for(const [id] of Object.entries(s.remoteFundLogs||{}))if(!currentLogs[id]){markTombstone(s,'fundLog',id,'cashtop');await removeLocalFundLog(id)}for(const [id,meta] of Object.entries(s.remoteVouchers||{}))if(!currentVouchers[id]){markTombstone(s,'voucher',id,'cashtop');await removeLocalExternal('voucher',id);if(meta.relationId)affected.add(`customers:${meta.relationId}`)}for(const [id,meta] of Object.entries(s.remoteSupplierMovements||{}))if(!currentMoves[id]){markTombstone(s,'supplierMovement',id,'cashtop');await removeLocalExternal('supplierMovement',id);if(meta.supplierId)affected.add(`suppliers:${meta.supplierId}`)}}s.remoteInvoices=currentInvoices;s.remoteFundLogs=currentLogs;s.remoteVouchers=currentVouchers;s.remoteSupplierMovements=currentMoves;return affected}

function trimState(s){const trimObj=(obj,max)=>{const e=Object.entries(obj||{});if(e.length<=max)return obj;return Object.fromEntries(e.sort((a,b)=>num(b[1]?.at||b[1])-num(a[1]?.at||a[1])).slice(0,max))};s.seenCash=trimObj(s.seenCash,12000);s.seenParty=trimObj(s.seenParty,20000);s.pushedLocal=trimObj(s.pushedLocal,10000);s.tombstones=trimObj(s.tombstones,20000);s.pendingDeletes=trimObj(s.pendingDeletes,5000);s.remoteInvoices=trimObj(s.remoteInvoices,20000);s.remoteFundLogs=trimObj(s.remoteFundLogs,30000);s.remoteVouchers=trimObj(s.remoteVouchers,20000);s.remoteSupplierMovements=trimObj(s.remoteSupplierMovements,30000);return s}

async function pullCashTop(linkInfo,s,{baseline=false}={}){
  const groupId=await activeFinancialGroup(linkInfo.tenantId);if(groupId!==linkInfo.financialGroupId){linkInfo.financialGroupId=groupId;save(LINK_KEY,linkInfo);baseline=true}
  const [{data:customers},{data:suppliers},{data:vouchers},{data:funds},{data:purchases},{data:invoices},{data:salesReturns},{data:purchaseReturns},{data:materialPurchases}]=await Promise.all([
    readDataset(linkInfo.tenantId,'cashtop_customers',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_suppliers',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_vouchers',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_funds_db',groupId,{accounts:[],accountLogs:[]}).catch(()=>({data:{accounts:[],accountLogs:[]}})),
    readDataset(linkInfo.tenantId,'cashtop_purchases',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_invoices',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_sales_returns',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_purchase_returns',groupId,[]).catch(()=>({data:[]})),
    readDataset(linkInfo.tenantId,'cashtop_material_purchases',groupId,[]).catch(()=>({data:[]}))
  ]);
  const cs=normalizeArray(customers),ss=normalizeArray(suppliers),vs=normalizeArray(vouchers),logs=normalizeArray(actualObject(funds,{}).accountLogs),invRows=normalizeArray(invoices);
  await upgradeLegacyInvoiceRecords(logs,invRows);
  await upgradeV17InvoiceSplit(logs,invRows,s,baseline);
  const affected=await reconcileRemoteDeletions(s,{invoices:invRows,logs,vouchers:vs,suppliers:ss,baseline});
  if(!baseline){for(const log of logs){const sid=clean(log?.sourceId);if(String(log?.sourceType||'')==='sale-reversal'&&sid&&!rowById(invRows,sid)){markTombstone(s,'sale',sid,'cashtop');await removeLocalExternal('sale',sid)}}}
  for(const c of cs){const key=`customers:${c.id}`,bal=customerBalance(c),known=Object.prototype.hasOwnProperty.call(s.observedBalances,key),existing=findLocalByRemoteId('customers',c.id)||findLocalByName('customers',clean(c.name));if(baseline||!known){ensureLocalContact('customers',c,{createOpening:!existing&&Math.abs(bal)>.0001,balance:bal});await syncCustomerEvents(c,vs,invRows,s,true);s.observedBalances[key]=bal;continue}ensureLocalContact('customers',c);if(affected.has(key)){await syncCustomerEvents(c,vs,invRows,s,false);s.observedBalances[key]=bal;continue}const delta=bal-num(s.observedBalances[key]),eventEffect=await syncCustomerEvents(c,vs,invRows,s,false),residual=delta-eventEffect;if(Math.abs(residual)>.0001)await applyRemotePartyDelta('customers',c,residual,latestPartyHint('customers',c,vs));s.observedBalances[key]=bal}
  for(const sp of ss){const key=`suppliers:${sp.id}`,bal=supplierBalance(sp),known=Object.prototype.hasOwnProperty.call(s.observedBalances,key),existing=findLocalByRemoteId('suppliers',sp.id)||findLocalByName('suppliers',clean(sp.name));if(baseline||!known){ensureLocalContact('suppliers',sp,{createOpening:!existing&&Math.abs(bal)>.0001,balance:bal});await syncSupplierEvents(sp,s,true);s.observedBalances[key]=bal;continue}ensureLocalContact('suppliers',sp);if(affected.has(key)){await syncSupplierEvents(sp,s,false);s.observedBalances[key]=bal;continue}const delta=bal-num(s.observedBalances[key]),eventEffect=await syncSupplierEvents(sp,s,false),residual=delta-eventEffect;if(Math.abs(residual)>.0001)await applyRemotePartyDelta('suppliers',sp,residual,latestPartyHint('suppliers',sp,vs));s.observedBalances[key]=bal}
  const cashCtx={customers:cs,suppliers:ss,vouchers:vs,purchases,invoices:invRows,salesReturns,purchaseReturns,materialPurchases,fundLogs:logs};for(const log of logs)await importCashLog(log,cashCtx,s,baseline);
  saveData();try{renderDebtsList?.();renderCashList?.();if(currentDetailName)renderPersonDetailList?.()}catch(_){}
}

async function syncNow(manual=false,forceBaseline=false){
  const l=link();if(!l){if(manual)toastMsg('اربط كاش توب أولاً','info');return false}if(syncing)return false;if(!navigator.onLine){if(manual)toastMsg('لا يوجد اتصال بالإنترنت','info');return false}if(isExpired()){if(manual)toastMsg('اشتراكك انتهى','error');return false}
  syncing=true;updateCard('syncing');try{const s=state(),baseline=forceBaseline||l.baselinePending===true;await processPendingDeletes(l,s);await pullCashTop(l,s,{baseline});if(canWrite(false))await syncLocalContactsAndRecords(l,s);l.baselinePending=false;s.lastSyncAt=Date.now();save(LINK_KEY,l);saveState(trimState(s));updateCard('linked');if(manual)toastMsg('تمت مزامنة كاش توب والدفتر');return true}catch(e){console.error('[CashTopLink] sync',e);updateCard('error');if(manual)toastMsg(e.message||'تعذر مزامنة كاش توب','error');return false}finally{syncing=false}}
function scheduleSync(ms=600){clearTimeout(syncTimer);syncTimer=setTimeout(()=>syncNow(false),ms)}

async function connect(){
  if(!canWrite(true))return;const key=document.getElementById('ct2-link-key')?.value.trim(),username=document.getElementById('ct2-link-user')?.value.trim(),password=document.getElementById('ct2-link-pass')?.value||'';if(!key||!username||!password){toastMsg('أكمل المفتاح واسم المستخدم وكلمة المرور','error');return}
  const btn=document.getElementById('ct2-link-submit');if(btn){btn.disabled=true;btn.innerHTML='<i class="fas fa-circle-notch fa-spin"></i> جاري الربط...'}
  try{const info=await authenticateCashTop(key,username,password);save(LINK_KEY,info);saveState({seenCash:{},seenParty:{},observedBalances:{},pushedLocal:{},initializedContacts:{},tombstones:{},pendingDeletes:{},remoteInvoices:{},remoteFundLogs:{},remoteVouchers:{},remoteSupplierMovements:{},lastSyncAt:0});await syncNow(false,true);closeModal();updateCard('linked');toastMsg('تم الربط مع كاش توب')}
  catch(e){toastMsg(e.message||'تعذر الربط','error')}
  finally{if(btn){btn.disabled=false;btn.innerHTML='<i class="fas fa-link"></i> ربط كاش توب'}}
}
function disconnect(){const l=link();if(!l)return;Swal.fire({title:'إلغاء الربط؟',text:'لن تُحذف أي حسابات أو معاملات من أي تطبيق.',icon:'question',showCancelButton:true,confirmButtonText:'إلغاء الربط',cancelButtonText:'رجوع'}).then(r=>{if(!r.isConfirmed)return;localStorage.removeItem(scoped(LINK_KEY));localStorage.removeItem(scoped(STATE_KEY));updateCard('off');closeModal();toastMsg('تم إلغاء الربط','info')})}
function openModal(){const m=document.getElementById('cashTopLinkModal');if(!m)return;const l=link();document.getElementById('ct2-link-key').value=l?.companyKey||'';document.getElementById('ct2-link-user').value=l?.username||'';document.getElementById('ct2-link-pass').value='';const info=document.getElementById('ct2-linked-info'),dis=document.getElementById('ct2-unlink-btn');if(l){info.classList.remove('hidden');info.innerHTML=`<i class="fas fa-circle-check"></i><div><b>مرتبط بـ ${escapeHTML(l.companyName||'كاش توب')}</b><small>${escapeHTML(l.username||'')} • ${escapeHTML(l.financialGroupId||LEGACY_GROUP)}</small></div>`;dis.classList.remove('hidden')}else{info.classList.add('hidden');dis.classList.add('hidden')}m.classList.remove('hidden');requestAnimationFrame(()=>m.querySelector('.ct2-link-panel')?.classList.add('show'))}
function closeModal(){const m=document.getElementById('cashTopLinkModal');if(!m)return;m.querySelector('.ct2-link-panel')?.classList.remove('show');setTimeout(()=>m.classList.add('hidden'),180)}
function updateCard(mode){const l=link(),s=state(),status=document.getElementById('cashTopLinkStatus'),icon=document.getElementById('cashTopLinkStateIcon');if(!status)return;if(mode==='syncing'){status.textContent='جاري المزامنة...';return}if(l){status.textContent=s.lastSyncAt?`مرتبط • آخر مزامنة ${shortTime(s.lastSyncAt)}`:'مرتبط وجاهز';if(icon)icon.className='fas fa-circle-check text-success text-2xl mb-3'}else{status.textContent='المفتاح وبيانات الدخول';if(icon)icon.className='fas fa-link text-brand text-2xl mb-3'}}

function installTransactionHook(){
  const old=window.confirmKeypad;if(typeof old==='function'&&!old.__ct2Wrapped){const wrapped=async function(){if(!canWrite(true))return;const beforeDebt=new Set((records||[]).map(x=>String(x.id))),beforeCash=new Set((cashRecords||[]).map(x=>String(x.id)));const result=await old.apply(this,arguments);for(const r of records||[])if(!beforeDebt.has(String(r.id)))await registerLocalTransaction(r,'debt');for(const r of cashRecords||[])if(!beforeCash.has(String(r.id)))await registerLocalTransaction(r,'cash');scheduleSync(100);return result};wrapped.__ct2Wrapped=true;window.confirmKeypad=wrapped}
}
function guardWrites(){
  const names=['resetAllData','savePerson','saveNewCategory','confirmKeypad','archivePerson','deleteSingleTransaction','editSingleTransaction','finishCashSession','saveDebtReminderForm','markReminderDone','deleteReminder','handleProfileImage','saveSettings','importFullBackup','archiveSelectedPersonTransactions','restoreSelectedPersonArchive','deleteSelectedPersonArchive','restoreArchivedTransaction','saveCurrentPersonEdit','convertCurrentPersonType','deleteCurrentPersonFromEdit'];
  for(const n of names){const fn=window[n];if(typeof fn!=='function'||fn.__expiryGuard)continue;const w=function(){if(!canWrite(true))return;return fn.apply(this,arguments)};w.__expiryGuard=true;window[n]=w}
}
async function seedLedgerFromExisting(){
  if(isExpired()||typeof ledgerRecords==='undefined')return;
  const seeded=load('cashtop_ledger_seed_v16',false);if(seeded)return;
  for(const r of records||[]){if(!(num(r.debit)>0||num(r.credit)>0))continue;await appendLedger({id:`ledger:existing:party:${r.id}`,timestamp:r.timestamp,account:r.name,description:r.note||'حركة حساب',document:r.type==='customers'?'عميل':'مورد',debit:num(r.debit),credit:num(r.credit),source:r.externalSource==='cashtop2'?'كاش توب':'الدفتر',origin:r.externalSource==='cashtop2'?'cashtop2':'daftar-existing',entityType:'party',partyType:r.type,partyName:r.name,localRecordId:String(r.id),externalRef:r.externalRef||`existing:party:${r.id}`})}
  for(const r of cashRecords||[]){if(!(num(r.amount)>0))continue;await appendLedger({id:`ledger:existing:cash:${r.id}`,timestamp:r.timestamp,account:'دفتر النقدية',description:r.note||(r.type==='in'?'دخل':'مصروف'),document:'النقدية',debit:r.type==='out'?num(r.amount):0,credit:r.type==='in'?num(r.amount):0,source:r.externalSource==='cashtop2'?'كاش توب':'الدفتر',origin:r.externalSource==='cashtop2'?'cashtop2':'daftar-existing',entityType:'cash',localRecordId:String(r.id),externalRef:r.externalRef||`existing:cash:${r.id}`})}
  save('cashtop_ledger_seed_v16',true);
}
function installHooks(){installTransactionHook();guardWrites();seedLedgerFromExisting().catch(()=>{});if(autoTimer)clearInterval(autoTimer);autoTimer=setInterval(()=>syncNow(false),45000);addEventListener('online',()=>scheduleSync(700));addEventListener('focus',()=>scheduleSync(900));document.addEventListener('visibilitychange',()=>{if(!document.hidden)scheduleSync(900)});updateCard(link()?'linked':'off');if(link())scheduleSync(900)}

window.CashTopLink={open:openModal,close:closeModal,connect,disconnect,syncNow,scheduleSync,registerLocalTransaction,appendLedger,handleLocalDelete,isLinked:()=>!!link(),linkInfo:()=>link(),getInvoiceSnapshot:id=>{const sid=String(id||'');for(const r of cashRecords||[])if(r.detailType==='invoice'&&String(r.cashTop2SourceId||r.detailData?.id)===sid)return r.detailData||null;for(const r of records||[])if(r.detailType==='invoice'&&String(r.cashTop2SourceId||r.detailData?.id)===sid)return r.detailData||null;return null}};
document.addEventListener('DOMContentLoaded',()=>setTimeout(installHooks,180));
})();
