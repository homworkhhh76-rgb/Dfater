(function(){
'use strict';

const SESSION_KEY='cashtop_cloud_session_v3';
const API_KEY='cashtop_api_base_v3';
const DEFAULT_API='/api';
const MAX_OUTBOX_BATCH=100;
const AUTO_REMOTE_PULL_MS=90*1000;
const SESSION_VALIDATE_MS=60*1000;
const LAZY_REMOTE_CACHE_MS=90*1000;
const ARCHIVE_DOM_LIMIT=240;
const ARCHIVE_ROW_ESTIMATE=92;
let session=readSession();
let dbPromise=null;
let syncLock=false;
let autoSyncTimer=null;
let archiveObserver=null;
let archiveCursor={beforeTs:Number.MAX_SAFE_INTEGER,beforeId:'\uffff',done:false};
let archiveRendered=0;
let archiveTopDropped=0;
let currentArchivedRecord=null;
let currentArchivedDebtRecord=null;
let installPrompt=null;
let applyingRemote=false;
let lastValidateAt=Number(session?.lastValidatedAt||0);
let lazyRemoteCache=new Map();
let debtArchiveSummaryCache=new Map();
let forceNextRemotePull=true;
let lastSyncError='';
let localObjectUrls=new Set();

window.CashTopSync={
  login,logout,syncNow,queueLegacySnapshot,putEntity,deleteEntity,archivePage,
  storeArchiveSession,resolveRecordImage,chooseInstall,requestPersistentStorage,
  showStorageInfo,buildBackup,restoreBackup,hydrateLegacyFromIndexedDB,apiBase:()=>apiBase(),session:()=>session,
  archiveDebtRecords,listDebtArchive,debtArchiveSummary,openDebtArchivedTransaction,restoreDebtArchivedTransaction,deleteDebtArchivedTransaction,restoreCashArchivedTransaction,moveDebtArchivePerson,restoreDebtArchiveBatch,deleteDebtArchiveBatch,
  deleteEntityAndImage,deleteImageAsset,downloadRecordImage,isSubscriptionExpired,canWrite
};


function isSubscriptionExpired(){
  const exp=Number(session?.company?.expiresAt||0);
  return !!(exp&&Date.now()>=exp);
}
function canWrite(showToast=true){
  if(!isSubscriptionExpired())return true;
  if(showToast)try{toast('اشتراكك انتهى','error')}catch(_){}
  return false;
}

function readSession(){
  try{return JSON.parse(localStorage.getItem(SESSION_KEY)||'null')}catch{return null}
}
function writeSession(s){
  session=s||null;
  if(session)localStorage.setItem(SESSION_KEY,JSON.stringify(session)); else localStorage.removeItem(SESSION_KEY);
  window.__ctSession=session;
}
function apiBase(){return String(localStorage.getItem(API_KEY)||DEFAULT_API).replace(/\/+$/,'')}
function scopedKey(k){
  const cid=session?.company?.id||window.__ctSession?.company?.id||'guest';
  return `ct3:${cid}:${k}`;
}
window.ctScopedKey=scopedKey;

function idbName(){
  const cid=session?.company?.id;
  if(!cid)throw new Error('لا توجد شركة مسجلة');
  return `CashTopCloud_${cid}_v3`;
}
function openDb(){
  if(dbPromise)return dbPromise;
  dbPromise=new Promise((resolve,reject)=>{
    const req=indexedDB.open(idbName(),5);
    req.onupgradeneeded=()=>{
      const db=req.result;
      let items;
      if(!db.objectStoreNames.contains('items'))items=db.createObjectStore('items',{keyPath:'pk'});else items=req.transaction.objectStore('items');
      if(!items.indexNames.contains('kind_parent_sort'))items.createIndex('kind_parent_sort',['kind','parentId','sortTs']);
      if(!items.indexNames.contains('kind_parent_sort_id'))items.createIndex('kind_parent_sort_id',['kind','parentId','sortTs','id']);
      if(!items.indexNames.contains('kind_sort'))items.createIndex('kind_sort',['kind','sortTs']);
      if(!items.indexNames.contains('server_updated'))items.createIndex('server_updated','serverUpdatedAt');
      let out;
      if(!db.objectStoreNames.contains('outbox'))out=db.createObjectStore('outbox',{keyPath:'opId'});else out=req.transaction.objectStore('outbox');
      if(!out.indexNames.contains('createdAt'))out.createIndex('createdAt','createdAt');
      if(!out.indexNames.contains('kind'))out.createIndex('kind','kind');
      if(!out.indexNames.contains('entityKey'))out.createIndex('entityKey','entityKey');
      if(!db.objectStoreNames.contains('blobs'))db.createObjectStore('blobs',{keyPath:'id'});
      if(!db.objectStoreNames.contains('meta'))db.createObjectStore('meta',{keyPath:'key'});
    };
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error);
  });
  return dbPromise;
}
function txDone(tx){return new Promise((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error||new Error('IndexedDB aborted'))})}
function reqP(req){return new Promise((resolve,reject)=>{req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error)})}
function pk(kind,id){return `${kind}:${String(id)}`}
function newOpId(){return `${Date.now().toString(36)}-${crypto.randomUUID()}`}

async function getMeta(key,fallback=null){
  const db=await openDb(),tx=db.transaction('meta','readonly');
  const v=await reqP(tx.objectStore('meta').get(key));
  return v?.value??fallback;
}
async function setMeta(key,value){
  const db=await openDb(),tx=db.transaction('meta','readwrite');
  tx.objectStore('meta').put({key,value});await txDone(tx);
}
async function getItem(kind,id){
  const db=await openDb(),tx=db.transaction('items','readonly');
  return reqP(tx.objectStore('items').get(pk(kind,id)));
}
async function putItemOnly(kind,id,payload,{parentId='',sortTs=0,deleted=false,serverUpdatedAt=0}={}){
  const db=await openDb(),tx=db.transaction('items','readwrite');
  tx.objectStore('items').put({pk:pk(kind,id),kind,id:String(id),parentId:String(parentId||''),sortTs:Number(sortTs||0),payload:payload||{},deleted:!!deleted,serverUpdatedAt:Number(serverUpdatedAt||0)});
  await txDone(tx);
}
function cloudPayload(payload){
  const p={...(payload||{})};
  // Local blob identifiers and data URLs never leave the device. Turso receives
  // image URLs only after Bunny upload succeeds.
  delete p.imageLocalId;
  if(String(p.image||'').startsWith('data:image/'))delete p.image;
  if(String(p.imageUrl||'').startsWith('data:image/'))delete p.imageUrl;
  return p;
}
function payloadEquivalent(a,b){
  try{return JSON.stringify(a||{})===JSON.stringify(b||{})}catch{return false}
}
function entityQueueKey(kind,id){return `${String(kind)}:${String(id)}`}
async function queueLatestEntityOp(op){
  const db=await openDb(),tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox'),ek=entityQueueKey(op.kind,op.id),idx=store.index('entityKey');
  // Keep only the newest unsent state for an entity. The indexed lookup keeps
  // large archive batches O(1)-ish instead of scanning the whole queue each time.
  await new Promise((resolve,reject)=>{const req=idx.getAllKeys(ek);req.onerror=()=>reject(req.error);req.onsuccess=()=>{for(const key of req.result||[])store.delete(key);store.put({...op,entityKey:ek});resolve()}});
  await txDone(tx);
}
async function migrateOutboxEconomyV14(){
  if(await getMeta('outboxEconomyV14',false))return;
  const db=await openDb(),tx=db.transaction('outbox','readonly'),all=await reqP(tx.objectStore('outbox').getAll());
  const latest=new Map(),special=[];
  for(const op of all||[]){if(String(op.kind||'').startsWith('__')){special.push(op);continue}const ek=entityQueueKey(op.kind,op.id),prev=latest.get(ek);if(!prev||Number(op.createdAt||0)>=Number(prev.createdAt||0))latest.set(ek,{...op,entityKey:ek})}
  const tx2=db.transaction('outbox','readwrite'),store=tx2.objectStore('outbox');store.clear();for(const op of special)store.put(op);for(const op of latest.values())store.put(op);await txDone(tx2);await setMeta('outboxEconomyV14',true);
}

async function putEntity(kind,id,payload,{parentId='',sortTs=0,deleted=false,queue=true}={}){
  if(!session?.company?.id)return false;
  const existing=await getItem(kind,id).catch(()=>null),nextParent=String(parentId||''),nextSort=Number(sortTs||0),nextDeleted=!!deleted;
  const same=!!existing&&String(existing.parentId||'')===nextParent&&Number(existing.sortTs||0)===nextSort&&!!existing.deleted===nextDeleted&&payloadEquivalent(existing.payload,payload||{});
  if(same)return false; // no local change -> no cloud write
  const db=await openDb(),tx=db.transaction('items','readwrite');
  const item={pk:pk(kind,id),kind,id:String(id),parentId:nextParent,sortTs:nextSort,payload:payload||{},deleted:nextDeleted,serverUpdatedAt:0};
  tx.objectStore('items').put(item);await txDone(tx);
  if(queue){const opId=newOpId();await queueLatestEntityOp({opId,kind,id:String(id),parentId:item.parentId,sortTs:item.sortTs,payload:cloudPayload(item.payload),deleted:item.deleted,createdAt:Date.now()})}
  if(kind==='archive_record'||kind==='debt_archive_record'){lazyRemoteCache.clear();debtArchiveSummaryCache.clear()}
  updateSyncUI();scheduleAutoSync();return true;
}
async function deleteEntity(kind,id,{parentId='',sortTs=0}={}){
  return putEntity(kind,id,{}, {parentId,sortTs,deleted:true,queue:true});
}
async function enqueueImage(blobId,targetKind,targetId,parentId='',sortTs=0,replaceUrl=''){
  const db=await openDb(),tx=db.transaction('outbox','readwrite');
  const opId=`img-${newOpId()}`;
  tx.objectStore('outbox').put({opId,kind:'__image_upload__',createdAt:Date.now(),payload:{blobId,targetKind,targetId:String(targetId),parentId:String(parentId||''),sortTs:Number(sortTs||0),replaceUrl:String(replaceUrl||'')}});
  await txDone(tx);updateSyncUI();scheduleAutoSync();return opId;
}
async function saveBlob(blob){
  const db=await openDb(),tx=db.transaction('blobs','readwrite');
  const id=`blob-${crypto.randomUUID()}`;tx.objectStore('blobs').put({id,blob,createdAt:Date.now(),size:blob.size,type:blob.type});await txDone(tx);return id;
}
async function getBlob(id){if(!id)return null;const db=await openDb(),tx=db.transaction('blobs','readonly');return reqP(tx.objectStore('blobs').get(id))}
async function deleteBlob(id){if(!id)return;const db=await openDb(),tx=db.transaction('blobs','readwrite');tx.objectStore('blobs').delete(id);await txDone(tx)}

function remoteImageUrl(payload){
  const p=payload||{},u=String(p.imageUrl||p.image||'').trim();
  return u&&!u.startsWith('data:')&&!u.startsWith('blob:')?u:'';
}
async function cancelPendingImageUploads(targetKind,targetId){
  if(!targetKind||targetId===undefined||targetId===null)return [];
  const db=await openDb(),tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox'),blobIds=[];
  await new Promise((resolve,reject)=>{const req=store.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c){resolve();return}const x=c.value,p=x.payload||{};if(x.kind==='__image_upload__'&&String(p.targetKind)===String(targetKind)&&String(p.targetId)===String(targetId)){if(p.blobId)blobIds.push(String(p.blobId));c.delete()}c.continue()}});await txDone(tx);
  for(const id of [...new Set(blobIds)])await deleteBlob(id).catch(()=>{});
  return blobIds;
}
async function queueImageDelete(url){
  url=String(url||'').trim();if(!url||url.startsWith('data:')||url.startsWith('blob:'))return false;
  const db=await openDb(),tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let exists=false;
  await new Promise((resolve,reject)=>{const req=store.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c){resolve();return}const v=c.value;if(v.kind==='__image_delete__'&&String(v.payload?.url||'')===url)exists=true;c.continue()}});
  if(!exists)store.put({opId:`imgdel-${newOpId()}`,kind:'__image_delete__',createdAt:Date.now(),payload:{url}});await txDone(tx);if(!exists){updateSyncUI();scheduleAutoSync(250)}return !exists;
}
async function deleteImageAsset(payload,targetKind,targetId){
  const p=payload||{};await cancelPendingImageUploads(targetKind,targetId).catch(()=>{});
  if(p.imageLocalId)await deleteBlob(p.imageLocalId).catch(()=>{});
  const url=remoteImageUrl(p);if(url)await queueImageDelete(url);
  return !!(url||p.imageLocalId);
}
async function deleteEntityAndImage(kind,id,options={},payloadOverride=null){
  const item=payloadOverride?null:await getItem(kind,id).catch(()=>null),payload=payloadOverride||item?.payload||{};
  await deleteImageAsset(payload,kind,id);return deleteEntity(kind,id,options);
}
async function processImageDeletes(max=8){
  const ops=await getOutbox(max,x=>x.kind==='__image_delete__');
  for(const op of ops){try{await api('/images/delete',{method:'POST',body:{url:op.payload?.url||''},timeout:22000});await deleteOutbox([op.opId])}catch(e){lastSyncError=e.message;break}}
}
async function downloadRecordImage(record,filename='CashTop_Image.jpg'){
  const src=await resolveRecordImage(record);if(!src){toast('لا توجد صورة للتحميل','info');return false}
  try{
    const res=await fetch(src,{cache:'no-store'});if(!res.ok)throw new Error('download');const blob=await res.blob(),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=filename||`CashTop_Image_${Date.now()}.jpg`;document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove()},1200);return true;
  }catch(e){const a=document.createElement('a');a.href=src;a.download=filename||`CashTop_Image_${Date.now()}.jpg`;a.target='_blank';a.rel='noopener';document.body.appendChild(a);a.click();a.remove();return true}
}

async function outboxCount(){
  if(!session?.company?.id)return 0;const db=await openDb(),tx=db.transaction('outbox','readonly');return reqP(tx.objectStore('outbox').count())
}
async function getOutbox(limit=MAX_OUTBOX_BATCH,kindFilter=null){
  const db=await openDb(),tx=db.transaction('outbox','readonly'),idx=tx.objectStore('outbox').index('createdAt'),out=[];
  return new Promise((resolve,reject)=>{
    const req=idx.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{
      const c=req.result;if(!c||out.length>=limit){resolve(out);return}
      const v=c.value;if(!kindFilter||kindFilter(v))out.push(v);c.continue();
    };
  });
}
async function deleteOutbox(ids){
  if(!ids?.length)return;const db=await openDb(),tx=db.transaction('outbox','readwrite'),s=tx.objectStore('outbox');ids.forEach(id=>s.delete(id));await txDone(tx)
}

async function api(path,{method='GET',body,form,timeout=18000,allow401=false}={}){
  // v5: direct Turso/Bunny adapter is bundled, so no Worker or API setup is required.
  if(window.CashTopDirectAPI?.request){
    try{return await window.CashTopDirectAPI.request(path,{method,body,form,token:session?.token||''})}
    catch(e){if(Number(e?.status)===401&&!allow401)handleInvalidSession(e.message||'انتهت الجلسة');throw e}
  }
  const ctrl=new AbortController(),timer=setTimeout(()=>ctrl.abort(),timeout);
  try{
    const headers={};
    if(session?.token)headers.Authorization=`Bearer ${session.token}`;
    if(body!==undefined)headers['Content-Type']='application/json';
    const res=await fetch(apiBase()+path,{method,headers,body:form|| (body!==undefined?JSON.stringify(body):undefined),signal:ctrl.signal,cache:'no-store'});
    const data=await res.json().catch(()=>({ok:false,error:`HTTP ${res.status}`}));
    if(!res.ok){
      if(res.status===401&&!allow401){handleInvalidSession(data.error||'انتهت الجلسة');}
      const e=new Error(data.error||`HTTP ${res.status}`);e.status=res.status;throw e;
    }
    return data;
  }finally{clearTimeout(timer)}
}

async function login(companyKey){
  const key=String(companyKey||document.getElementById('login-company-key')?.value||'').trim();
  if(!key){showLoginMessage('أدخل مفتاح الشركة','error');return false}
  setLoginBusy(true);showLoginMessage('جاري التحقق من المفتاح...','info');
  try{
    const data=await api('/auth/login',{method:'POST',body:{companyKey:key},allow401:true});
    writeSession({token:data.token,company:data.company,loginAt:Date.now(),lastValidatedAt:Date.now()});
    localStorage.setItem('cashtop_last_company_hint',key.slice(0,5));
    location.replace('./app.html');return true;
  }catch(e){showLoginMessage(e.message||'تعذر تسجيل الدخول','error');return false}
  finally{setLoginBusy(false)}
}
function logout(reason='تم تسجيل الخروج'){
  writeSession(null);dbPromise=null;
  try{sessionStorage.clear()}catch{}
  location.replace('./index.html');
}
function handleInvalidSession(reason){
  try{localStorage.setItem('cashtop_logout_reason',reason||'الجلسة غير صالحة')}catch{}
  writeSession(null);setTimeout(()=>location.replace('./index.html'),100);
}
async function validateSession(force=false){
  if(!session)return false;
  // انتهاء الاشتراك لا ينهي الجلسة: القراءة والتصفح يبقيان متاحين.
  if(!navigator.onLine)return true;
  if(!force && Date.now()-lastValidateAt<SESSION_VALIDATE_MS)return true;
  try{
    const d=await api('/auth/validate');
    session.company=d.company;session.lastValidatedAt=Date.now();writeSession(session);lastValidateAt=Date.now();return true;
  }catch{return false}
}


function imageFolderForKind(kind){
  const k=String(kind||'misc');
  if(k==='profile')return 'company';
  if(k==='debt_record')return 'debts';
  if(k==='cash_record')return 'cash';
  if(k==='archive_record')return 'archive';
  if(k==='debt_archive_record')return 'debt-archive';
  return 'misc';
}

async function processImageUploads(max=4){
  const ops=await getOutbox(max,x=>x.kind==='__image_upload__');
  for(const op of ops){
    const p=op.payload||{},entry=await getBlob(p.blobId);
    if(!entry?.blob){await deleteOutbox([op.opId]);continue}
    const fd=new FormData();
    fd.append('file',entry.blob,`image-${Date.now()}.jpg`);
    fd.append('folder',imageFolderForKind(p.targetKind));
    fd.append('entityId',String(p.targetId||'item'));
    try{
      const data=await api('/images/upload',{method:'POST',form:fd,timeout:30000});
      const item=await getItem(p.targetKind,p.targetId);
      if(item&&!item.deleted){
        const payload={...(item.payload||{}),image:data.url,imageUrl:data.url,imageLocalId:''};
        await putEntity(p.targetKind,p.targetId,payload,{parentId:item.parentId||p.parentId,sortTs:item.sortTs||p.sortTs,queue:true});
        applyImageUrlToLegacy(p.targetKind,p.targetId,data.url);
      }else{
        // The record may have been deleted while the upload was in flight. Do not leave an orphan file in Bunny.
        await queueImageDelete(data.url);
      }
      if(p.replaceUrl&&String(p.replaceUrl)!==String(data.url))await queueImageDelete(p.replaceUrl);
      await deleteOutbox([op.opId]);
      const remaining=await getOutbox(500,x=>x.kind==='__image_upload__'&&x.payload?.blobId===p.blobId);
      if(!remaining.length)await deleteBlob(p.blobId);
    }catch(e){lastSyncError=e.message;break}
  }
}
function applyImageUrlToLegacy(kind,id,url){
  const sid=String(id);
  if(kind==='cash_record'){
    const r=cashRecords.find(x=>String(x.id)===sid);if(r){r.image=url;r.imageUrl=url;r.imageLocalId='';saveData()}
  }else if(kind==='debt_record'){
    const r=records.find(x=>String(x.id)===sid);if(r){r.image=url;r.imageUrl=url;r.imageLocalId='';saveData()}
  }else if(kind==='profile'){
    profile.image=url;profile.imageUrl=url;profile.imageLocalId='';saveData();renderProfileUI?.()
  }else if(kind==='archive_record'&&currentArchivedRecord&&String(currentArchivedRecord.id)===sid){
    currentArchivedRecord.image=url;currentArchivedRecord.imageUrl=url;currentArchivedRecord.imageLocalId='';
  }else if(kind==='debt_archive_record'&&currentArchivedDebtRecord&&String(currentArchivedDebtRecord.id)===sid){
    currentArchivedDebtRecord.image=url;currentArchivedDebtRecord.imageUrl=url;currentArchivedDebtRecord.imageLocalId='';
  }
}

async function pushOutbox(){
  const ops=await getOutbox(MAX_OUTBOX_BATCH,x=>x.kind!=='__image_upload__'&&x.kind!=='__image_delete__');
  if(!ops.length)return 0;
  const payload=ops.map(x=>({opId:x.opId,kind:x.kind,id:x.id,parentId:x.parentId||'',sortTs:Number(x.sortTs||0),payload:x.payload||{},deleted:!!x.deleted}));
  const d=await api('/sync/push',{method:'POST',body:{ops:payload},timeout:25000});
  await deleteOutbox(d.acked||[]);return (d.acked||[]).length;
}
async function pullRemote(manual=false){
  const cur=await getMeta('pullCursor',{t:0,rowid:0});
  let cursor={t:Number(cur?.t||0),rowid:Number(cur?.rowid||0)};
  let pages=0,changed=0,received=0,caughtUp=false;
  do{
    const q=`?t=${encodeURIComponent(cursor.t)}&rowid=${encodeURIComponent(cursor.rowid)}&limit=${manual?500:250}`;
    const d=await api('/sync/pull'+q,{timeout:25000});
    for(const it of d.items||[]){received++;if(await mergeRemoteItem(it))changed++}
    cursor=d.cursor||cursor;await setMeta('pullCursor',cursor);pages++;caughtUp=!d.hasMore;
    if(!d.hasMore||pages>=(manual?12:2))break;
  }while(true);
  await setMeta('lastRemotePullAt',Date.now());
  if(changed){saveData();refreshVisibleUI()}
  return{changed,received,caughtUp,cursor};
}
async function mergeRemoteItem(it){
  const old=await getItem(it.kind,it.id).catch(()=>null),same=!!old&&String(old.parentId||'')===String(it.parentId||'')&&Number(old.sortTs||0)===Number(it.sortTs||0)&&!!old.deleted===!!it.deleted&&payloadEquivalent(old.payload,it.payload||{});
  await putItemOnly(it.kind,it.id,it.payload,{parentId:it.parentId,sortTs:it.sortTs,deleted:it.deleted,serverUpdatedAt:it.serverUpdatedAt});
  if(same)return false;
  applyingRemote=true;try{applyRemoteToLegacy(it)}finally{applyingRemote=false}return true;
}
function applyRemoteToLegacy(it){
  const id=String(it.id),p=it.payload||{};
  const upsert=(arr)=>{const i=arr.findIndex(x=>String(x.id)===id);if(it.deleted){if(i>=0)arr.splice(i,1)}else if(i>=0)arr[i]=p;else arr.push(p)};
  if(it.kind==='debt_record')upsert(records);
  else if(it.kind==='cash_record')upsert(cashRecords);
  else if(it.kind==='archive_session')upsert(cashArchives);
  else if(it.kind==='category')upsert(categories);
  else if(it.kind==='reminder')upsert(reminders);
  else if(it.kind==='ledger_record'&&typeof ledgerRecords!=='undefined')upsert(ledgerRecords);
  else if(it.kind==='contact'){
    if(it.deleted)delete contacts[id]; else contacts[id]=p;
  }else if(it.kind==='profile'&&!it.deleted){profile={...profile,...p}}
}

function refreshVisibleUI(){
  try{renderDebtsList?.();renderCashList?.();renderProfileUI?.();if(currentDetailName)renderPersonDetailList?.();if(document.getElementById('archiveModal')?.classList.contains('show'))renderArchiveSessions?.()}catch(e){console.warn(e)}
}

async function syncNow(manual=false){
  if(syncLock||!session?.token)return false;
  if(!navigator.onLine){lastSyncError='لا يوجد اتصال';updateSyncUI();if(manual)toast('أنت الآن دون اتصال','info');return false}
  syncLock=true;setSyncVisual('syncing');
  try{
    if(!(await validateSession(manual)))return false;
    const lastPull=Number(await getMeta('lastRemotePullAt',0)||0),remoteDue=manual||forceNextRemotePull||!lastPull||(Date.now()-lastPull>=AUTO_REMOTE_PULL_MS);
    // Pull only when due/manual. Local edits still PUSH immediately, so normal
    // usage no longer pays for a Turso read after every single transaction.
    if(remoteDue){await pullRemote(manual);forceNextRemotePull=false}
    if(!isSubscriptionExpired()){
      await processImageDeletes(manual?16:5);
      await processImageUploads(manual?8:3);
      await processImageDeletes(manual?16:5);
      let loops=0;
      do{const n=await pushOutbox();loops++;if(!n||loops>=(manual?12:2))break}while(true);
    }
    await setMeta('lastSuccessfulSync',Date.now());lastSyncError='';updateSyncUI();
    if(manual)toast(isSubscriptionExpired()?'تم تحديث بيانات العرض — اشتراكك انتهى':'تمت المزامنة بنجاح',isSubscriptionExpired()?'info':'success');return true;
  }catch(e){lastSyncError=e.message||'فشلت المزامنة';console.warn('sync',e);updateSyncUI();if(manual)toast(lastSyncError,'error');return false}
  finally{syncLock=false;setSyncVisual('idle')}
}
function scheduleAutoSync(delay=1200){clearTimeout(autoSyncTimer);autoSyncTimer=setTimeout(()=>syncNow(false),delay)}

async function updateSyncUI(){
  const count=await outboxCount().catch(()=>0);
  const badge=document.getElementById('sync-queue-badge');
  if(badge){badge.textContent=count>999?'999+':String(count);badge.classList.toggle('hidden',!count)}
  const status=document.getElementById('sync-status-text');
  if(status)status.textContent=isSubscriptionExpired()?'عرض فقط':(navigator.onLine?(lastSyncError?'خطأ مزامنة':'متصل'):'دون اتصال');
  const queue=document.getElementById('settings-sync-queue');if(queue)queue.textContent=String(count);
  const online=document.getElementById('settings-online-state');if(online)online.textContent=navigator.onLine?'أونلاين':'أوفلاين';
  const err=document.getElementById('settings-sync-error');if(err)err.textContent=lastSyncError||'—';
}
function setSyncVisual(state){const i=document.getElementById('sync-icon');if(i)i.classList.toggle('fa-spin',state==='syncing')}

// ---------------- Archive: IndexedDB + lazy loading ----------------
async function storeArchiveSession(sessionMeta,recordsToArchive){
  const meta={...sessionMeta};delete meta.records;
  await putEntity('archive_session',meta.id,meta,{sortTs:meta.closedAt||Date.now(),queue:true});
  for(let i=0;i<recordsToArchive.length;i++){
    const r={...recordsToArchive[i],archiveSessionId:meta.id};
    await putEntity('archive_record',r.id,r,{parentId:String(meta.id),sortTs:r.timestamp||0,queue:true});
    await deleteEntity('cash_record',r.id,{sortTs:r.timestamp||0});
    await rebindPendingImage(r.id,meta.id);
    if(i%50===49)await new Promise(requestAnimationFrame);
  }
}
async function rebindPendingImage(recordId,sessionId){
  const db=await openDb(),tx=db.transaction('outbox','readwrite'),s=tx.objectStore('outbox');
  await new Promise((resolve,reject)=>{const req=s.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c){resolve();return}const x=c.value;if(x.kind==='__image_upload__'&&String(x.payload?.targetKind)==='cash_record'&&String(x.payload?.targetId)===String(recordId)){x.payload.targetKind='archive_record';x.payload.parentId=String(sessionId);c.update(x)}c.continue()}});
  await txDone(tx)
}
function lazyCacheFresh(key){const t=Number(lazyRemoteCache.get(key)||0);return t&&Date.now()-t<LAZY_REMOTE_CACHE_MS}
function markLazyCache(key){lazyRemoteCache.set(key,Date.now());if(lazyRemoteCache.size>200){const first=lazyRemoteCache.keys().next().value;if(first)lazyRemoteCache.delete(first)}}
async function archivePage(sessionId,beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',limit=50){
  let local=await readArchiveLocal(sessionId,beforeTs,beforeId,limit),cacheKey=`cash:${sessionId}:${beforeTs}:${beforeId}:${limit}`;
  if(navigator.onLine&&session?.token&&!(local.length>=limit&&lazyCacheFresh(cacheKey))){
    try{
      const q=`?kind=archive_record&parentId=${encodeURIComponent(sessionId)}&beforeTs=${encodeURIComponent(beforeTs)}&beforeId=${encodeURIComponent(beforeId)}&limit=${limit}`;
      const d=await api('/items/list'+q,{timeout:22000});
      for(const it of d.items||[]){const pending=await getItem(it.kind,it.id);if(pending&&Number(pending.serverUpdatedAt||0)===0)continue;await putItemOnly(it.kind,it.id,it.payload,{parentId:it.parentId,sortTs:it.sortTs,deleted:it.deleted,serverUpdatedAt:it.serverUpdatedAt})}
      markLazyCache(cacheKey);local=await readArchiveLocal(sessionId,beforeTs,beforeId,limit);
    }catch(e){console.warn('lazy archive',e)}
  }
  return local;
}
async function readArchiveLocal(sessionId,beforeTs,beforeId,limit){
  const db=await openDb(),tx=db.transaction('items','readonly'),idx=tx.objectStore('items').index('kind_parent_sort_id');
  const sid=String(sessionId),topTs=Number(beforeTs),topId=String(beforeId||'\uffff');
  // Bound the cursor to this archive session only. This is crucial when the local
  // database contains millions of rows because IndexedDB never scans other sessions.
  const range=IDBKeyRange.bound(['archive_record',sid,-Number.MAX_SAFE_INTEGER,''],['archive_record',sid,topTs,topId],false,true);
  const out=[];
  return new Promise((resolve,reject)=>{
    const req=idx.openCursor(range,'prev');
    req.onerror=()=>reject(req.error);
    req.onsuccess=()=>{
      const c=req.result;if(!c||out.length>=limit){resolve(out);return}
      const v=c.value;if(!v.deleted)out.push(v.payload);c.continue();
    }
  })
}
async function getArchiveRecord(sessionId,id){
  const it=await getItem('archive_record',id);if(it&&!it.deleted)return it.payload;
  if(navigator.onLine){
    const s=cashArchives.find(x=>String(x.id)===String(sessionId));
    const page=await archivePage(sessionId,(s?.closedAt||Date.now())+1,'\uffff',120);
    return page.find(x=>String(x.id)===String(id))||null;
  }
  return null;
}


async function readKindParentLocal(kind,parentId,beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',limit=50){
  const db=await openDb(),tx=db.transaction('items','readonly'),idx=tx.objectStore('items').index('kind_parent_sort_id');
  const k=String(kind),pid=String(parentId||''),topTs=Number(beforeTs),topId=String(beforeId||'\uffff');
  const range=IDBKeyRange.bound([k,pid,-Number.MAX_SAFE_INTEGER,''],[k,pid,topTs,topId],false,true),out=[];
  return new Promise((resolve,reject)=>{const req=idx.openCursor(range,'prev');req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c||out.length>=limit){resolve(out);return}const v=c.value;if(!v.deleted)out.push(v.payload);c.continue()}})
}
async function listDebtArchive(parentId,beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',limit=50){
  let local=await readKindParentLocal('debt_archive_record',parentId,beforeTs,beforeId,limit),cacheKey=`debt:${parentId}:${beforeTs}:${beforeId}:${limit}`;
  if(navigator.onLine&&!(local.length>=limit&&lazyCacheFresh(cacheKey))){
    try{const q=`?kind=debt_archive_record&parentId=${encodeURIComponent(parentId)}&beforeTs=${encodeURIComponent(beforeTs)}&beforeId=${encodeURIComponent(beforeId)}&limit=${limit}`;const d=await api('/items/list'+q,{timeout:22000});for(const it of d.items||[]){const pending=await getItem(it.kind,it.id);if(pending&&Number(pending.serverUpdatedAt||0)===0)continue;await putItemOnly(it.kind,it.id,it.payload,{parentId:it.parentId,sortTs:it.sortTs,deleted:it.deleted,serverUpdatedAt:it.serverUpdatedAt})}markLazyCache(cacheKey);local=await readKindParentLocal('debt_archive_record',parentId,beforeTs,beforeId,limit)}catch(e){console.warn('debt archive lazy',e)}
  }
  return local;
}

async function retargetPendingImage(recordId,fromKind,toKind,parentId=''){
  const db=await openDb(),tx=db.transaction('outbox','readwrite'),store=tx.objectStore('outbox');let found=false;
  await new Promise((resolve,reject)=>{const req=store.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c){resolve();return}const x=c.value;if(x.kind==='__image_upload__'&&String(x.payload?.targetKind)===String(fromKind)&&String(x.payload?.targetId)===String(recordId)){x.payload.targetKind=String(toKind);x.payload.parentId=String(parentId||'');c.update(x);found=true}c.continue()}});await txDone(tx);return found;
}
async function debtArchiveSummary(parentId){
  const ck=String(parentId||''),cached=debtArchiveSummaryCache.get(ck);if(cached&&Date.now()-cached.at<LAZY_REMOTE_CACHE_MS)return cached.value;
  if(navigator.onLine&&session?.token){try{const value=await api('/items/debt-archive-summary?parentId='+encodeURIComponent(parentId),{timeout:18000});debtArchiveSummaryCache.set(ck,{at:Date.now(),value});return value}catch(e){console.warn('debt archive summary',e)}}
  const rows=await readKindParentLocal('debt_archive_record',parentId,Number.MAX_SAFE_INTEGER,'\uffff',500);let totalCredit=0,totalDebit=0;rows.forEach(r=>{totalCredit+=Number(r.credit||0);totalDebit+=Number(r.debit||0)});return{ok:true,totalCredit,totalDebit,balance:totalCredit-totalDebit,count:rows.length,partial:rows.length>=500};
}
async function archiveDebtRecords(parentId,rows){
  const list=Array.isArray(rows)?rows:[];if(!list.length)return 0;
  for(const src of list){const r={...src,archivedAt:Date.now(),archiveParentId:String(parentId)};await putEntity('debt_archive_record',r.id,r,{parentId:String(parentId),sortTs:r.timestamp||Date.now(),queue:true});if(r.imageLocalId){const moved=await retargetPendingImage(r.id,'debt_record','debt_archive_record',String(parentId));if(!moved)await enqueueImage(r.imageLocalId,'debt_archive_record',r.id,String(parentId),r.timestamp||Date.now())}await deleteEntity('debt_record',r.id,{parentId:String(parentId),sortTs:r.timestamp||0})}
  return list.length;
}
async function getDebtArchiveRecord(id){const it=await getItem('debt_archive_record',id);return it&&!it.deleted?it.payload:null}

async function moveDebtArchivePerson(oldParentId,newParentId,newType,newName){
  oldParentId=String(oldParentId||'');newParentId=String(newParentId||'');
  if(!oldParentId||!newParentId||oldParentId===newParentId)return 0;
  let beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',moved=0,guard=0;
  while(guard++<100000){
    const page=await listDebtArchive(oldParentId,beforeTs,beforeId,100);
    if(!page.length)break;
    for(const src of page){
      const r={...src,type:newType||src.type,name:newName||src.name,archiveParentId:newParentId};
      await putEntity('debt_archive_record',r.id,r,{parentId:newParentId,sortTs:r.timestamp||0,queue:true});
      if(r.imageLocalId)await retargetPendingImage(r.id,'debt_archive_record','debt_archive_record',newParentId);
      moved++;
    }
    const last=page[page.length-1];beforeTs=Number(last.timestamp||0);beforeId=String(last.id);
    if(page.length<100)break;
  }
  return moved;
}
async function restoreDebtArchiveBatch(parentId,rows){
  const list=Array.isArray(rows)?rows:[];const restored=[];
  for(const src of list){
    const r={...src};delete r.archivedAt;delete r.archiveParentId;
    await deleteEntity('debt_archive_record',r.id,{parentId:String(parentId||personKey(r.name,r.type)),sortTs:r.timestamp||0});
    const activeParent=personKey(r.name,r.type);
    await putEntity('debt_record',r.id,r,{parentId:activeParent,sortTs:r.timestamp||0,queue:true});
    if(r.imageLocalId){const moved=await retargetPendingImage(r.id,'debt_archive_record','debt_record',activeParent);if(!moved)await enqueueImage(r.imageLocalId,'debt_record',r.id,activeParent,r.timestamp||0)}
    restored.push(r);
  }
  return restored;
}
async function deleteDebtArchiveBatch(parentId,rows){
  const list=Array.isArray(rows)?rows:[];let count=0;
  for(const r of list){await deleteEntityAndImage('debt_archive_record',r.id,{parentId:String(parentId||personKey(r.name,r.type)),sortTs:r.timestamp||0},r);count++}
  return count;
}
async function openDebtArchivedTransaction(id){
  currentTransId=id;currentTransSource='debtArchive';currentArchivedDebtRecord=await getDebtArchiveRecord(id);window.__ctCurrentArchivedDebtRecord=currentArchivedDebtRecord;if(!currentArchivedDebtRecord){toast('تعذر تحميل المعاملة المؤرشفة','error');return}
  renderDebtArchivedTransaction(currentArchivedDebtRecord);
}
function renderDebtArchivedTransaction(r){
  const out=Number(r.debit)>0,amt=Number(out?r.debit:r.credit)||0;
  document.getElementById('trans-name').innerText=r.name||currentDetailName||'معاملة مؤرشفة';document.getElementById('trans-time').innerText=`${todayLabel(r.timestamp)} ساعة ${shortTime(r.timestamp)}`;
  document.getElementById('trans-type-text').innerText=out?'أخذت':'أعطيت';document.getElementById('trans-amount').innerText=`₪ ${fmtMoney(amt)}`;document.getElementById('trans-amount').className=`text-5xl font-extrabold mb-2 ${out?'text-success':'text-danger'}`;
  const b=document.getElementById('trans-balance');b.innerText='معاملة محفوظة في الأرشيف';b.className='text-sm font-bold mb-4 px-2 rounded bg-blue-50 text-brand';
  const nw=document.getElementById('trans-note-wrap');if(r.note){document.getElementById('trans-note').innerText=r.note;nw.classList.remove('hidden')}else nw.classList.add('hidden');
  renderTransactionProfile?.();showTransactionImage(r);const m=document.getElementById('singleTransactionModal');m.classList.remove('hidden');m.classList.add('flex');window.updateTransactionArchiveActions?.();
}
async function restoreDebtArchivedTransaction(){
  const r=currentArchivedDebtRecord;if(!r)return null;const parentId=personKey(r.name,r.type);await deleteEntity('debt_archive_record',r.id,{parentId,sortTs:r.timestamp||0});const restored={...r,archivedAt:undefined,archiveParentId:undefined};await putEntity('debt_record',r.id,restored,{parentId,sortTs:r.timestamp||0,queue:true});if(restored.imageLocalId){const moved=await retargetPendingImage(restored.id,'debt_archive_record','debt_record',parentId);if(!moved)await enqueueImage(restored.imageLocalId,'debt_record',restored.id,parentId,restored.timestamp||0)}return restored;
}
async function deleteDebtArchivedTransaction(){
  const r=currentArchivedDebtRecord;if(!r)return false;await deleteEntityAndImage('debt_archive_record',r.id,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0},r);currentArchivedDebtRecord=null;window.__ctCurrentArchivedDebtRecord=null;return true;
}

window.renderArchiveSessions=function(){
  const list=document.getElementById('archive-session-list');if(!list)return;
  const arr=[...cashArchives].sort((a,b)=>Number(b.closedAt||0)-Number(a.closedAt||0));
  if(!arr.length){list.innerHTML='<div class="text-center py-20 text-gray-400"><div class="archive-empty-icon"><i class="fas fa-box-archive"></i></div><div class="font-extrabold text-lg">لا توجد جلسات مؤرشفة</div><div class="text-sm mt-2">عند إنهاء دفتر النقدية تظهر كل جلسة هنا بشكل مستقل.</div></div>';return}
  const stamp=(ts)=>{const d=new Date(Number(ts)||Date.now()),pad=n=>String(n).padStart(2,'0');return `${pad(d.getDate())}/${pad(d.getMonth()+1)}/${String(d.getFullYear()).slice(-2)} • ${pad(d.getHours())}:${pad(d.getMinutes())}`};
  list.innerHTML=arr.map(s=>{const tin=Number(s.totalIn||0),tout=Number(s.totalOut||0),bal=Number(s.net??(tin-tout));return `<button type="button" onclick="openArchiveSession('${escapeHTML(String(s.id))}')" class="archive-session-card archive-v9-card"><div class="archive-v9-line"><span class="archive-v9-label">من</span><span class="archive-v9-stamp">${stamp(s.openedAt)}</span></div><div class="archive-v9-line"><span class="archive-v9-label">إلى</span><span class="archive-v9-stamp">${stamp(s.closedAt)}</span></div><div class="archive-v9-bottom"><div class="archive-v9-balance ${bal>=0?'positive':'negative'}"><span>الرصيد الصافي</span><strong>₪ ${fmtMoney(Math.abs(bal))}</strong></div><div class="archive-v9-side"><span>${Number(s.recordCount||0)} معاملة</span><i class="fas fa-chevron-left"></i></div></div></button>`}).join('');
};
window.openArchive=function(sessionId=null){
  document.getElementById('archiveModal')?.classList.add('show');renderArchiveSessions();if(sessionId)setTimeout(()=>openArchiveSession(sessionId),30)
};
window.closeArchive=function(){document.getElementById('archiveSessionModal')?.classList.remove('show');document.getElementById('archiveModal')?.classList.remove('show');cleanupArchiveObserver()};
window.openArchiveSession=async function(id){
  currentArchiveSessionId=id;currentArchiveTab='overview';
  document.getElementById('archiveSessionModal')?.classList.add('show');
  renderArchiveSessionSummary();setArchiveTab('overview');await resetArchiveDetails();
};
window.closeArchiveSession=function(){document.getElementById('archiveSessionModal')?.classList.remove('show');cleanupArchiveObserver();renderArchiveSessions()};
window.setArchiveTab=function(tab){
  currentArchiveTab=tab;
  document.getElementById('archive-tab-overview')?.classList.toggle('active',tab==='overview');
  document.getElementById('archive-tab-details')?.classList.toggle('active',tab==='details');
  document.getElementById('archive-overview')?.classList.toggle('hidden',tab!=='overview');
  document.getElementById('archive-details-wrap')?.classList.toggle('hidden',tab!=='details');
  if(tab==='details')setTimeout(()=>ensureArchiveObserver(),20);
};
function renderArchiveSessionSummary(){
  const s=cashArchives.find(x=>String(x.id)===String(currentArchiveSessionId));if(!s)return;
  const tin=Number(s.totalIn||0),tout=Number(s.totalOut||0),bal=Number(s.net??(tin-tout));
  const od=document.getElementById('archive-open-date');if(od)od.textContent=typeof archiveStamp==='function'?archiveStamp(s.openedAt):formatDateTime(Number(s.openedAt||0));
  const cd=document.getElementById('archive-close-date');if(cd)cd.textContent=typeof archiveStamp==='function'?archiveStamp(s.closedAt):formatDateTime(Number(s.closedAt||0));
  const o=document.getElementById('archive-overview');if(o)o.innerHTML=`<div class="soft-card p-5"><div class="text-brand font-extrabold text-lg mb-4">الرصيد</div><div class="text-4xl font-extrabold ${bal>=0?'text-success':'text-danger'} mb-6" dir="ltr">₪ ${fmtMoney(Math.abs(bal))}</div><div class="border-t pt-4"><div class="flex justify-between py-3"><b class="text-success text-xl" dir="ltr">₪ ${fmtMoney(tin)}</b><div class="text-right"><div class="text-brand font-bold">إجمالي النقد الداخل</div><div class="text-xs text-gray-400">${Number(s.inCount||0)} عملية</div></div></div><div class="flex justify-between py-3 border-t"><b class="text-danger text-xl" dir="ltr">₪ ${fmtMoney(tout)}</b><div class="text-right"><div class="text-brand font-bold">إجمالي النقد الخارج</div><div class="text-xs text-gray-400">${Number(s.outCount||0)} عملية</div></div></div></div></div>`;
}
async function resetArchiveDetails(){
  cleanupArchiveObserver();archiveCursor={beforeTs:Number.MAX_SAFE_INTEGER,beforeId:'\uffff',done:false};archiveRendered=0;archiveTopDropped=0;
  const list=document.getElementById('archive-details');if(!list)return;list.innerHTML='';
  const spacer=document.getElementById('archive-top-spacer');if(spacer)spacer.style.height='0px';
  await loadNextArchivePage();ensureArchiveObserver();
}
function pageSizeForScreen(){return Math.min(72,Math.max(30,Math.ceil(window.innerHeight/78)*4))}
async function loadNextArchivePage(){
  if(archiveCursor.done||!currentArchiveSessionId)return;
  const sentinel=document.getElementById('archive-load-sentinel');if(sentinel)sentinel.innerHTML='<i class="fas fa-circle-notch fa-spin"></i> جاري تحميل المزيد';
  const page=await archivePage(currentArchiveSessionId,archiveCursor.beforeTs,archiveCursor.beforeId,pageSizeForScreen());
  if(!page.length){archiveCursor.done=true;if(sentinel)sentinel.textContent=archiveRendered?'نهاية السجل':'لا توجد معاملات';return}
  appendArchiveRows(page);const last=page[page.length-1];archiveCursor.beforeTs=Number(last.timestamp||0);archiveCursor.beforeId=String(last.id);archiveRendered+=page.length;
  if(page.length<pageSizeForScreen())archiveCursor.done=true;
  if(sentinel)sentinel.textContent=archiveCursor.done?'نهاية السجل':'اسحب للأسفل للمزيد';
}
function appendArchiveRows(rows){
  const list=document.getElementById('archive-details');if(!list)return;
  for(const r of rows){
    const incoming=r.type==='in',hasImg=!!(r.image||r.imageUrl||r.imageLocalId);
    const div=document.createElement('div');div.className='archive-record-row archive-row';div.dataset.recordId=String(r.id);
    div.onclick=()=>openArchivedTransaction(currentArchiveSessionId,r.id);
    div.innerHTML=`<div class="archive-record-amount"><strong class="${incoming?'text-success':'text-danger'}" dir="ltr">₪ ${fmtMoney(r.amount)}</strong><span>${todayLabel(r.timestamp)} • ${shortTime(r.timestamp)}</span></div><div class="archive-record-main"><div class="text-right"><b>${incoming?'دخل':'مصروف'}</b><small>${escapeHTML(r.note||'')}</small></div>${hasImg?`<div class="archive-image-slot" data-local-image="${escapeHTML(r.imageLocalId||'')}" data-url-image="${escapeHTML(r.imageUrl||r.image||'')}"><i class="fas fa-image"></i></div>`:`<div class="archive-type-icon ${incoming?'in':'out'}"><i class="fas ${incoming?'fa-plus':'fa-minus'}"></i></div>`}</div>`;
    list.appendChild(div);hydrateImageSlot(div.querySelector('.archive-image-slot'));
  }
  trimArchiveDom();
}
function trimArchiveDom(){
  const list=document.getElementById('archive-details');if(!list)return;
  const rows=[...list.querySelectorAll('.archive-record-row')];
  if(rows.length<=ARCHIVE_DOM_LIMIT)return;
  const removeCount=rows.length-ARCHIVE_DOM_LIMIT,scroller=document.getElementById('archive-details-scroll');
  const oldTop=Number(scroller?.scrollTop||0);
  rows.slice(0,removeCount).forEach(x=>x.remove());archiveTopDropped+=removeCount;
  // Do not create a multi-million-pixel spacer: browsers have maximum layout heights.
  // Keep a bounded DOM window and compensate scroll position instead.
  if(scroller)scroller.scrollTop=Math.max(0,oldTop-removeCount*ARCHIVE_ROW_ESTIMATE);
  const spacer=document.getElementById('archive-top-spacer');if(spacer)spacer.style.height='0px';
  const note=document.getElementById('archive-window-note');if(note){note.classList.remove('hidden');note.textContent=`تم إبقاء ${ARCHIVE_DOM_LIMIT} سجلاً فقط في الذاكرة لتجنب التعليق. اضغط لإعادة أحدث السجلات.`;note.onclick=()=>resetArchiveDetails()}
}
function ensureArchiveObserver(){
  cleanupArchiveObserver();const s=document.getElementById('archive-load-sentinel');if(!s)return;
  archiveObserver=new IntersectionObserver(es=>{if(es.some(e=>e.isIntersecting))loadNextArchivePage()},{root:document.getElementById('archive-details-scroll'),rootMargin:'500px'});archiveObserver.observe(s)
}
function cleanupArchiveObserver(){if(archiveObserver){archiveObserver.disconnect();archiveObserver=null}}
window.resetArchiveWindow=()=>resetArchiveDetails();

window.openArchivedTransaction=async function(sessionId,id){
  currentArchiveSessionId=sessionId;currentTransId=id;currentTransSource='archive';
  currentArchivedRecord=await getArchiveRecord(sessionId,id);window.__ctCurrentArchivedRecord=currentArchivedRecord;if(!currentArchivedRecord){toast('تعذر تحميل المعاملة','error');return}
  renderArchivedTransaction(currentArchivedRecord);
};
const legacyGetCurrentTransaction=typeof getCurrentTransaction==='function'?getCurrentTransaction:null;
window.getCurrentTransaction=function(){if(currentTransSource==='archive')return currentArchivedRecord;if(currentTransSource==='debtArchive')return currentArchivedDebtRecord;return legacyGetCurrentTransaction?legacyGetCurrentTransaction():null};
function renderArchivedTransaction(r){
  const out=r.type==='out',amt=Number(r.amount||0);
  document.getElementById('trans-name').innerText=r.note||(out?'مصروف':'دخل');document.getElementById('trans-time').innerText=`${todayLabel(r.timestamp)} ساعة ${shortTime(r.timestamp)}`;
  document.getElementById('trans-type-text').innerText=out?'مصروف':'دخل';document.getElementById('trans-amount').innerText=`₪ ${fmtMoney(amt)}`;document.getElementById('trans-amount').className=`text-5xl font-extrabold mb-2 ${out?'text-danger':'text-success'}`;
  const b=document.getElementById('trans-balance');b.innerText=`الرصيد بعد الأرشفة`;b.className='text-sm font-bold mb-4 px-2 rounded bg-blue-50 text-brand';
  const nw=document.getElementById('trans-note-wrap');if(r.note){document.getElementById('trans-note').innerText=r.note;nw.classList.remove('hidden')}else nw.classList.add('hidden');
  renderTransactionProfile?.();showTransactionImage(r);
  const m=document.getElementById('singleTransactionModal');m.classList.remove('hidden');m.classList.add('flex');window.updateTransactionArchiveActions?.();
}
async function showTransactionImage(r){
  const iw=document.getElementById('trans-image-wrap'),img=document.getElementById('trans-image');if(!iw||!img)return;
  const src=await resolveRecordImage(r);if(src){img.src=src;iw.classList.remove('hidden')}else iw.classList.add('hidden')
}
async function resolveRecordImage(r){
  const remote=r?.imageUrl||r?.image||'';if(remote&&!String(remote).startsWith('data:'))return remote;
  if(r?.imageLocalId){const e=await getBlob(r.imageLocalId);if(e?.blob){const u=URL.createObjectURL(e.blob);localObjectUrls.add(u);return u}}
  if(String(remote).startsWith('data:'))return remote;return ''
}
async function hydrateImageSlot(slot){
  if(!slot)return;let src=slot.dataset.urlImage||'';
  if(!src&&slot.dataset.localImage){const e=await getBlob(slot.dataset.localImage);if(e?.blob){src=URL.createObjectURL(e.blob);localObjectUrls.add(src)}}
  if(src)slot.innerHTML=`<img src="${escapeHTML(src)}" alt="صورة المعاملة">`;
}

const legacyEditSingle=editSingleTransaction;
window.editSingleTransaction=async function(){
  if(currentTransSource==='debtArchive'){const r=currentArchivedDebtRecord;if(!r)return;const out=Number(r.debit)>0,amount=Number(out?r.debit:r.credit)||0;const {value:data}=await Swal.fire({title:'تعديل المعاملة المؤرشفة',html:`<input id="et-amount" type="number" step="0.01" min="0.01" class="swal2-input" value="${amount}"><div class="grid grid-cols-2 gap-2 mx-8 my-3"><label class="border rounded-xl p-3"><input type="radio" name="et-kind" value="credit" ${!out?'checked':''}> أعطيت</label><label class="border rounded-xl p-3"><input type="radio" name="et-kind" value="debit" ${out?'checked':''}> أخذت</label></div><input id="et-note" class="swal2-input" value="${escapeHTML(r.note||'')}" placeholder="ملاحظة">`,showCancelButton:true,confirmButtonText:'حفظ',cancelButtonText:'إلغاء',preConfirm:()=>({amount:Number(document.getElementById('et-amount').value),type:document.querySelector('input[name="et-kind"]:checked')?.value,note:document.getElementById('et-note').value.trim()})});if(!data||data.amount<=0)return;r.debit=data.type==='debit'?data.amount:0;r.credit=data.type==='credit'?data.amount:0;r.note=data.note;await putEntity('debt_archive_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0,queue:true});renderDebtArchivedTransaction(r);toast('تم تعديل المعاملة');return}
  if(currentTransSource!=='archive'){
    const source=currentTransSource,id=currentTransId;
    const before=source==='cash'?cashRecords.find(x=>String(x.id)===String(id)):records.find(x=>String(x.id)===String(id));
    const snapshot=before?JSON.stringify(before):'';
    await legacyEditSingle();
    const after=source==='cash'?cashRecords.find(x=>String(x.id)===String(id)):records.find(x=>String(x.id)===String(id));
    if(after&&JSON.stringify(after)!==snapshot){
      if(source==='cash')await putEntity('cash_record',after.id,after,{sortTs:after.timestamp||0,queue:true});
      else await putEntity('debt_record',after.id,after,{parentId:personKey(after.name,after.type),sortTs:after.timestamp||0,queue:true});
    }
    return;
  }
  const r=currentArchivedRecord;if(!r)return;
  const {value:data}=await Swal.fire({title:'تعديل المعاملة المؤرشفة',html:`<input id="et-amount" type="number" step="0.01" min="0.01" class="swal2-input" value="${Number(r.amount||0)}"><div class="grid grid-cols-2 gap-2 mx-8 my-3"><label class="border rounded-xl p-3"><input type="radio" name="et-kind" value="in" ${r.type==='in'?'checked':''}> دخل</label><label class="border rounded-xl p-3"><input type="radio" name="et-kind" value="out" ${r.type==='out'?'checked':''}> مصروف</label></div><input id="et-note" class="swal2-input" value="${escapeHTML(r.note||'')}" placeholder="ملاحظة">`,showCancelButton:true,confirmButtonText:'حفظ',cancelButtonText:'إلغاء',preConfirm:()=>({amount:Number(document.getElementById('et-amount').value),type:document.querySelector('input[name="et-kind"]:checked')?.value,note:document.getElementById('et-note').value.trim()})});
  if(!data||data.amount<=0)return;const before={...r};r.amount=data.amount;r.type=data.type;r.note=data.note;
  await putEntity('archive_record',r.id,r,{parentId:String(currentArchiveSessionId),sortTs:r.timestamp,queue:true});await adjustArchiveSessionSummary(currentArchiveSessionId,before,r);renderArchivedTransaction(r);await resetArchiveDetails();toast('تم تعديل المعاملة')
};
const legacyDeleteSingle=deleteSingleTransaction;
window.deleteSingleTransaction=async function(){
  if(currentTransSource==='debtArchive'){const before=currentArchivedDebtRecord?{...currentArchivedDebtRecord}:null;const c=await Swal.fire({title:'حذف المعاملة المؤرشفة؟',text:'سيتم حذفها نهائياً من الأرشيف ومن كاش توب إذا كانت حركة مرتبطة.',icon:'warning',showCancelButton:true,confirmButtonText:'حذف',confirmButtonColor:'#ef4444',cancelButtonText:'إلغاء'});if(!c.isConfirmed)return;await deleteDebtArchivedTransaction();if(before)await window.CashTopLink?.handleLocalDelete?.(before).catch(e=>console.warn('linked delete',e));closeSingleTransaction();window.refreshPersonArchiveAfterMutation?.();toast('تم حذف المعاملة');return}
  if(currentTransSource!=='archive'){
    const source=currentTransSource,id=currentTransId;
    const before=source==='cash'?cashRecords.find(x=>String(x.id)===String(id)):records.find(x=>String(x.id)===String(id));
    await legacyDeleteSingle();
    const still=source==='cash'?cashRecords.some(x=>String(x.id)===String(id)):records.some(x=>String(x.id)===String(id));
    if(before&&!still){
      if(source==='cash')await deleteEntityAndImage('cash_record',id,{sortTs:before.timestamp||0},before);
      else await deleteEntityAndImage('debt_record',id,{parentId:personKey(before.name,before.type),sortTs:before.timestamp||0},before);
      await window.CashTopLink?.handleLocalDelete?.(before).catch(e=>console.warn('linked delete',e));
    }
    return;
  }
  const r=currentArchivedRecord;if(!r)return;const before={...r};const c=await Swal.fire({title:'حذف المعاملة المؤرشفة؟',text:'سيتم حذفها من هذه الشركة ومن كاش توب إذا كانت حركة مرتبطة.',icon:'warning',showCancelButton:true,confirmButtonText:'حذف',confirmButtonColor:'#ef4444',cancelButtonText:'إلغاء'});if(!c.isConfirmed)return;
  await deleteEntityAndImage('archive_record',r.id,{parentId:String(currentArchiveSessionId),sortTs:r.timestamp},r);await adjustArchiveSessionSummary(currentArchiveSessionId,r,null);await window.CashTopLink?.handleLocalDelete?.(before).catch(e=>console.warn('linked delete',e));closeSingleTransaction();await resetArchiveDetails();toast('تم حذف المعاملة')
};

async function restoreCashArchivedTransaction(){
  const r=currentArchivedRecord;if(!r)return null;
  const restored={...r};delete restored.archiveSessionId;
  await deleteEntity('archive_record',r.id,{parentId:String(currentArchiveSessionId),sortTs:r.timestamp||0});
  await putEntity('cash_record',restored.id,restored,{sortTs:restored.timestamp||Date.now(),queue:true});
  if(restored.imageLocalId){const moved=await retargetPendingImage(restored.id,'archive_record','cash_record','');if(!moved)await enqueueImage(restored.imageLocalId,'cash_record',restored.id,'',restored.timestamp||Date.now())}
  await adjustArchiveSessionSummary(currentArchiveSessionId,r,null);
  currentArchivedRecord=null;window.__ctCurrentArchivedRecord=null;
  return restored;
}
async function adjustArchiveSessionSummary(sessionId,oldRec,newRec){
  const s=cashArchives.find(x=>String(x.id)===String(sessionId));if(!s)return;
  const apply=(r,sign)=>{if(!r)return;const a=Number(r.amount||0);if(r.type==='in'){s.totalIn=Math.max(0,Number(s.totalIn||0)+sign*a);s.inCount=Math.max(0,Number(s.inCount||0)+sign)}else{s.totalOut=Math.max(0,Number(s.totalOut||0)+sign*a);s.outCount=Math.max(0,Number(s.outCount||0)+sign)}};
  apply(oldRec,-1);apply(newRec,1);
  if(oldRec&&!newRec)s.recordCount=Math.max(0,Number(s.recordCount||0)-1);
  s.net=Number(s.totalIn||0)-Number(s.totalOut||0);
  saveData();await putEntity('archive_session',s.id,{...s},{sortTs:s.closedAt||Date.now(),queue:true});renderArchiveSessionSummary();renderArchiveSessions();
}
async function recomputeArchiveSession(sessionId){
  const s=cashArchives.find(x=>String(x.id)===String(sessionId));if(!s)return;
  // Keep summary correct for normal-sized local sessions. For very large cross-device
  // sessions the authoritative summary is metadata and we adjust deltas during edits/deletes.
  let tin=0,tout=0,count=0,inCount=0,outCount=0,beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff';
  for(let pages=0;pages<40;pages++){
    const page=await readArchiveLocal(sessionId,beforeTs,beforeId,100);if(!page.length)break;
    page.forEach(r=>{count++;if(r.type==='in'){tin+=Number(r.amount||0);inCount++}else{tout+=Number(r.amount||0);outCount++}});
    const last=page[page.length-1];beforeTs=Number(last.timestamp||0);beforeId=String(last.id);if(page.length<100)break;
    if(count>=4000)break;
  }
  if(count<4000||Number(s.recordCount||0)<=4000){s.totalIn=tin;s.totalOut=tout;s.net=tin-tout;s.recordCount=count;s.inCount=inCount;s.outCount=outCount;await putEntity('archive_session',s.id,{...s},{sortTs:s.closedAt,queue:true});saveData();renderArchiveSessionSummary();renderArchiveSessions()}
}

// ---------------- Image compression <= 50 KiB ----------------
// v16: نحافظ على الصورة كاملة ونسبة أبعادها. لا يوجد قص مربع إطلاقاً.
// نخفض جودة الترميز أولاً، وإذا كانت الصورة ضخمة جداً نخفض الدقة تناسبياً فقط كحل أخير للوصول إلى 50KB.
async function compressImageUnder50KB(file,maxKB=50){
  if(!(file instanceof Blob))throw new Error('ملف الصورة غير صالح');
  const limitBytes=Math.max(8,Number(maxKB||50))*1024;
  if(file.size<=limitBytes&&/^image\/(jpeg|jpg|webp)$/i.test(file.type||''))return file;
  const bitmap=await createBitmap(file),width=Number(bitmap.width||bitmap.naturalWidth||0),height=Number(bitmap.height||bitmap.naturalHeight||0);
  if(!width||!height){bitmap.close?.();throw new Error('أبعاد الصورة غير صالحة')}
  let scale=1,best=null;
  try{
    while(scale>=.16){
      const w=Math.max(1,Math.round(width*scale)),h=Math.max(1,Math.round(height*scale)),c=document.createElement('canvas');c.width=w;c.height=h;
      const x=c.getContext('2d',{alpha:false});if(!x)throw new Error('Canvas غير متاح');x.fillStyle='#fff';x.fillRect(0,0,w,h);x.drawImage(bitmap,0,0,width,height,0,0,w,h);
      for(const type of ['image/webp','image/jpeg']){
        for(let q=.9;q>=.02;q-=.04){
          const blob=await canvasBlob(c,type,Math.max(.01,q));if(!best||blob.size<best.size)best=blob;if(blob.size<=limitBytes)return blob;
        }
      }
      scale*=.86;
    }
    if(best&&best.size<=limitBytes)return best;
    throw new Error('تعذر ضغط الصورة إلى 50KB أو أقل');
  }finally{try{bitmap.close?.()}catch{}}
}

function canvasBlob(c,type,q){return new Promise((resolve,reject)=>c.toBlob(b=>b?resolve(b):reject(new Error('تعذر ضغط الصورة')),type,q))}
async function createBitmap(file){
  if(typeof createImageBitmap==='function'){
    try{return await createImageBitmap(file,{imageOrientation:'from-image'})}catch{}
  }
  return await new Promise((resolve,reject)=>{
    const reader=new FileReader();reader.onerror=()=>reject(reader.error||new Error('تعذر قراءة الصورة'));
    reader.onload=()=>{const img=new Image();img.onload=()=>{img.close=()=>{};resolve(img)};img.onerror=()=>reject(new Error('تعذر فتح الصورة'));img.src=reader.result};
    reader.readAsDataURL(file);
  });
}

window.handleTransactionImage=async function(event){
  const file=event.target.files?.[0];event.target.value='';if(!file)return;if(!file.type.startsWith('image/')){toast('اختر صورة فقط','error');return}
  try{
    const blob=await compressImageUnder50KB(file,50),blobId=await saveBlob(blob),preview=URL.createObjectURL(blob);localObjectUrls.add(preview);
    pendingImageData={blobId,preview,size:blob.size};document.getElementById('image-btn-label').innerText=`صورة ${(blob.size/1024).toFixed(1)}KB`;toast('تم ضغط الصورة وحفظها محلياً')
  }catch(e){toast(e.message||'تعذر ضغط الصورة','error')}
};
window.confirmKeypad=async function(){
  let amount=0;try{amount=evaluateExpression(calcExpression)}catch{}if(amount<=0){toast('أدخل مبلغاً صحيحاً','error');return}
  const note=document.getElementById('keypad-note').value.trim()||document.getElementById('keypad-header-title').innerText,now=Date.now();
  if(keypadType==='in'||keypadType==='out'){
    const r={id:now,type:keypadType,amount,note,catName:selectedCategory?.name||'',catIcon:selectedCategory?.icon||'fa-tag',image:'',imageUrl:'',imageLocalId:pendingImageData?.blobId||'',date:'اليوم',time:shortTime(now),timestamp:now};
    cashRecords.push(r);saveData();await putEntity('cash_record',r.id,r,{sortTs:r.timestamp,queue:true});if(r.imageLocalId)await enqueueImage(r.imageLocalId,'cash_record',r.id,'',r.timestamp);renderCashList();closeKeypad();showReceiptScreen(amount,keypadType,'دفتر النقدية');
  }else{
    if(!currentDetailName){toast('اختر عميلاً أو مورداً أولاً','error');return}
    const r={id:now,type:currentDebtTab,name:currentDetailName,note,debit:keypadType==='debit'?amount:0,credit:keypadType==='credit'?amount:0,image:'',imageUrl:'',imageLocalId:pendingImageData?.blobId||'',date:'اليوم',time:shortTime(now),timestamp:now};
    records.push(r);saveData();await putEntity('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp,queue:true});if(r.imageLocalId)await enqueueImage(r.imageLocalId,'debt_record',r.id,personKey(r.name,r.type),r.timestamp);renderPersonDetailList();renderDebtsList();closeKeypad();showReceiptScreen(amount,keypadType,currentDetailName)
  }
};

window.finishCashSession=async function(){
  if(!cashRecords.length){toast('لا توجد معاملات لإنهائها','info');return}
  let tin=0,tout=0,inCount=0,outCount=0;cashRecords.forEach(r=>{if(r.type==='in'){tin+=Number(r.amount);inCount++}else{tout+=Number(r.amount);outCount++}});
  const res=await Swal.fire({title:'إنهاء دفتر النقدية؟',html:`<div class="text-right leading-8">سيتم إنشاء جلسة مستقلة في الأرشيف تحتوي <b>${cashRecords.length}</b> معاملة ثم تصفير دفتر النقدية.<br>الدخل: <b>${moneyRaw(tin)} ₪</b><br>المصروف: <b>${moneyRaw(tout)} ₪</b></div>`,icon:'question',showCancelButton:true,confirmButtonText:'إنهاء وأرشفة',cancelButtonText:'إلغاء'});if(!res.isConfirmed)return;
  const sorted=[...cashRecords].sort((a,b)=>a.timestamp-b.timestamp),closed=Date.now();
  const sessionMeta={id:closed,openedAt:sorted[0]?.timestamp||closed,closedAt:closed,totalIn:tin,totalOut:tout,net:tin-tout,recordCount:cashRecords.length,inCount,outCount};
  const copy=cashRecords.map(x=>({...x}));cashArchives.push(sessionMeta);cashRecords=[];saveData();renderCashList();
  Swal.fire({title:'جاري الأرشفة محلياً...',allowOutsideClick:false,didOpen:()=>Swal.showLoading()});
  try{await storeArchiveSession(sessionMeta,copy);Swal.close();toast('تمت الأرشفة وتصفير دفتر النقدية');setTimeout(()=>openArchive(sessionMeta.id),120)}catch(e){Swal.close();toast('تم الحفظ محلياً لكن تعذر تجهيز طابور المزامنة','error');console.error(e)}
};

// Profile images also use Bunny, not Turso blobs.
window.handleProfileImage=async function(event){
  const file=event.target.files?.[0];event.target.value='';if(!file)return;
  try{const blob=await compressImageUnder50KB(file,50),blobId=await saveBlob(blob),preview=URL.createObjectURL(blob);localObjectUrls.add(preview);pendingProfileImage={blobId,preview,size:blob.size};renderSettingsAvatar();toast('تم ضغط الشعار محلياً')}catch(e){toast(e.message||'تعذر ضغط الصورة','error')}
};
window.renderSettingsAvatar=function(){
  const w=document.getElementById('settings-avatar-wrap'),b=document.getElementById('settings-logo-download');if(!w)return;
  const src=typeof pendingProfileImage==='object'?pendingProfileImage.preview:(pendingProfileImage||profile.imageUrl||profile.image||'');w.className=src?'profile-avatar overflow-hidden':'profile-placeholder';w.innerHTML=src?`<img src="${escapeHTML(src)}" class="w-full h-full object-cover" alt="">`:'<i class="fas fa-store"></i>';if(b)b.classList.toggle('hidden',!src);
};
const legacyOpenSettings=openSettings;
window.openSettings=function(){
  pendingProfileImage=profile.image||'';legacyOpenSettings();updateSyncUI();showStorageInfo();updateInstallButton();
};
window.saveSettings=async function(){
  const name=document.getElementById('settings-name').value.trim()||'كاش توب',phone=document.getElementById('settings-phone').value.trim(),address=document.getElementById('settings-address').value.trim();
  const imgObj=typeof pendingProfileImage==='object'?pendingProfileImage:null,oldProfile={...profile},oldLogoUrl=remoteImageUrl(oldProfile);
  if(imgObj){await cancelPendingImageUploads('profile','main').catch(()=>{});if(oldProfile.imageLocalId)await deleteBlob(oldProfile.imageLocalId).catch(()=>{})}
  profile={...profile,name,phone,address};if(imgObj){profile.image='';profile.imageUrl='';profile.imageLocalId=imgObj.blobId}else if(typeof pendingProfileImage==='string'){profile.image=pendingProfileImage;profile.imageUrl=String(pendingProfileImage||'');}
  saveData();await putEntity('profile','main',profile,{sortTs:Date.now(),queue:true});if(imgObj)await enqueueImage(imgObj.blobId,'profile','main','',Date.now(),oldLogoUrl);renderProfileUI();closeSettings();toast('تم حفظ الإعدادات')
};

// ---------------- One-time legacy migration ----------------
async function seedIfMissing(kind,id,payload,parentId='',sortTs=0){
  const old=await getItem(kind,id);if(old)return;await putEntity(kind,id,payload,{parentId,sortTs,queue:true})
}
async function prepareLegacyImage(obj){
  if(!obj||!String(obj.image||obj.imageUrl||'').startsWith('data:image/'))return '';
  const data=String(obj.image||obj.imageUrl);
  try{
    const original=await dataUrlToBlob(data);
    const blob=original.size<=50*1024?original:await compressImageUnder50KB(new File([original],'legacy.jpg',{type:original.type||'image/jpeg'}),50);
    const blobId=await saveBlob(blob);obj.image='';obj.imageUrl='';obj.imageLocalId=blobId;return blobId;
  }catch(e){console.warn('legacy image',e);obj.image='';obj.imageUrl='';return ''}
}
async function migrateLegacy(){
  if(await getMeta('legacyMigrated',false))return;
  await requestPersistentStorage();
  const oldArchives=[];
  for(const r of records){const blobId=await prepareLegacyImage(r);await seedIfMissing('debt_record',r.id,r,personKey(r.name,r.type),r.timestamp||0);if(blobId)await enqueueImage(blobId,'debt_record',r.id,personKey(r.name,r.type),r.timestamp||0)}
  for(const r of cashRecords){const blobId=await prepareLegacyImage(r);await seedIfMissing('cash_record',r.id,r,'',r.timestamp||0);if(blobId)await enqueueImage(blobId,'cash_record',r.id,'',r.timestamp||0)}
  for(const c of categories)await seedIfMissing('category',c.id,c,'',c.id||0);
  for(const [k,c] of Object.entries(contacts))await seedIfMissing('contact',k,c,'',0);
  for(const r of reminders)await seedIfMissing('reminder',r.id,r,personKey(r.name,r.type),r.at||0);
  const profileBlobId=await prepareLegacyImage(profile);await seedIfMissing('profile','main',profile,'',0);if(profileBlobId)await enqueueImage(profileBlobId,'profile','main','',0);
  for(const s of cashArchives){
    if(Array.isArray(s.records)&&s.records.length){
      let tin=0,tout=0,inCount=0,outCount=0;s.records.forEach(r=>{if(r.type==='in'){tin+=Number(r.amount||0);inCount++}else{tout+=Number(r.amount||0);outCount++}});
      const meta={...s,totalIn:tin,totalOut:tout,net:tin-tout,recordCount:s.records.length,inCount,outCount};delete meta.records;oldArchives.push(meta);
      await seedIfMissing('archive_session',meta.id,meta,'',meta.closedAt||0);
      for(const r of s.records){const blobId=await prepareLegacyImage(r);const ar={...r,archiveSessionId:meta.id};await seedIfMissing('archive_record',r.id,ar,String(meta.id),r.timestamp||0);if(blobId)await enqueueImage(blobId,'archive_record',r.id,String(meta.id),r.timestamp||0)}
    }else{const meta={...s};delete meta.records;await seedIfMissing('archive_session',meta.id,meta,'',meta.closedAt||0)}
  }
  if(oldArchives.length){
    cashArchives=cashArchives.map(s=>{const m=oldArchives.find(x=>String(x.id)===String(s.id));if(m)return m;const x={...s};delete x.records;return x});
  }
  saveData();await setMeta('legacyMigrated',true);scheduleAutoSync(800)
}
async function dataUrlToBlob(dataUrl){const r=await fetch(dataUrl);return r.blob()}
async function queueLegacySnapshot(){return true}


async function getAllKindPayloads(kind){
  const db=await openDb(),tx=db.transaction('items','readonly'),idx=tx.objectStore('items').index('kind_sort'),out=[];
  const range=IDBKeyRange.bound([String(kind),-Number.MAX_SAFE_INTEGER],[String(kind),Number.MAX_SAFE_INTEGER]);
  return new Promise((resolve,reject)=>{const req=idx.openCursor(range,'prev');req.onerror=()=>reject(req.error);req.onsuccess=()=>{const c=req.result;if(!c){resolve(out);return}const v=c.value;if(!v.deleted)out.push(v.payload);c.continue()}});
}
function mergeById(target,incoming){const map=new Map(target.map(x=>[String(x.id),x]));for(const x of incoming||[])if(x&&x.id!==undefined)map.set(String(x.id),x);target.splice(0,target.length,...map.values());}
async function hydrateLegacyFromIndexedDB(){
  try{
    const [debt,cash,archives,cats,rems,ledger,contactRows,profiles]=await Promise.all(['debt_record','cash_record','archive_session','category','reminder','ledger_record','contact','profile'].map(getAllKindPayloads));
    mergeById(records,debt);mergeById(cashRecords,cash);mergeById(cashArchives,archives);mergeById(categories,cats);mergeById(reminders,rems);if(typeof ledgerRecords!=='undefined')mergeById(ledgerRecords,ledger);
    for(const c of contactRows||[]){const k=c?.__contactKey||c?.key;if(k)contacts[k]=c}
    // contact payloads from older versions don't contain their key; read the store directly for those.
    const db=await openDb(),tx=db.transaction('items','readonly'),store=tx.objectStore('items');
    await new Promise((resolve,reject)=>{const req=store.openCursor();req.onerror=()=>reject(req.error);req.onsuccess=()=>{const cur=req.result;if(!cur){resolve();return}const v=cur.value;if(v.kind==='contact'&&!v.deleted)contacts[String(v.id)]=v.payload;cur.continue()}});
    if(profiles?.length)profile={...profile,...profiles[0]};
    saveData();refreshVisibleUI();return true;
  }catch(e){console.warn('hydrate idb',e);return false}
}
async function migrateCategoryScopesV10(){
  if(await getMeta('categoryScopeMigratedV10',false))return;
  try{
    if(typeof migrateCategoryScopes==='function')migrateCategoryScopes();
    for(const c of categories)await putEntity('category',c.id,c,{sortTs:Number(c.id)||Date.now(),queue:true});
    for(const [k,c] of Object.entries(contacts||{}))await putEntity('contact',k,c,{queue:true});
    saveData();await setMeta('categoryScopeMigratedV10',true);scheduleAutoSync(250);
  }catch(e){console.warn('category scope migration v10',e)}
}

async function buildBackup(){
  if(navigator.onLine)await syncNow(false);
  const map=new Map((await getAllKindPayloads('archive_record')).map(r=>[String(r.id),r]));
  // Archive records are lazy by design, so a full backup explicitly walks every session.
  // This can take time for a huge archive but it avoids silently producing an incomplete backup.
  for(const s of cashArchives){
    let beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',guard=0;
    while(guard++<100000){
      const page=await archivePage(s.id,beforeTs,beforeId,120);
      if(!page.length)break;for(const r of page)map.set(String(r.id),r);
      const last=page[page.length-1];beforeTs=Number(last.timestamp||0);beforeId=String(last.id);
      if(page.length<120)break;
    }
  }
  const archiveRecords=[...map.values()];
  const debtArchiveMap=new Map((await getAllKindPayloads('debt_archive_record')).map(r=>[String(r.id),r]));
  const parents=new Set([...Object.keys(contacts),...records.map(r=>personKey(r.name,r.type))]);
  for(const parentId of parents){let beforeTs=Number.MAX_SAFE_INTEGER,beforeId='\uffff',guard=0;while(guard++<100000){const page=await listDebtArchive(parentId,beforeTs,beforeId,120);if(!page.length)break;for(const r of page)debtArchiveMap.set(String(r.id),r);const last=page[page.length-1];beforeTs=Number(last.timestamp||0);beforeId=String(last.id);if(page.length<120)break}}
  const debtArchiveRecords=[...debtArchiveMap.values()];
  return {backupVersion:'cashtop-cloud-v8',exportedAt:new Date().toISOString(),company:{id:session?.company?.id||'',name:session?.company?.name||''},data:{records:[...records],cashRecords:[...cashRecords],cashArchives:[...cashArchives],categories:[...categories],contacts:{...contacts},reminders:[...reminders],ledgerRecords:typeof ledgerRecords!=='undefined'?[...ledgerRecords]:[],profile:{...profile},archiveRecords,debtArchiveRecords}};
}
async function restoreBackup(doc){
  if(!doc||typeof doc!=='object'||!doc.data)throw new Error('ملف النسخة غير صالح');
  const d=doc.data;const debt=Array.isArray(d.records)?d.records:[],cash=Array.isArray(d.cashRecords)?d.cashRecords:[],archives=Array.isArray(d.cashArchives)?d.cashArchives:[],cats=Array.isArray(d.categories)?d.categories:[],rems=Array.isArray(d.reminders)?d.reminders:[],ledger=Array.isArray(d.ledgerRecords)?d.ledgerRecords:[],ars=Array.isArray(d.archiveRecords)?d.archiveRecords:[],debtArs=Array.isArray(d.debtArchiveRecords)?d.debtArchiveRecords:[];
  mergeById(records,debt);mergeById(cashRecords,cash);mergeById(cashArchives,archives);mergeById(categories,cats);mergeById(reminders,rems);if(typeof ledgerRecords!=='undefined')mergeById(ledgerRecords,ledger);Object.assign(contacts,d.contacts&&typeof d.contacts==='object'?d.contacts:{});if(d.profile&&typeof d.profile==='object')profile={...profile,...d.profile};saveData();
  let total=0;
  for(const r of debt){await putEntity('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  for(const r of cash){await putEntity('cash_record',r.id,r,{sortTs:r.timestamp||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  for(const s of archives){await putEntity('archive_session',s.id,s,{sortTs:s.closedAt||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  for(const r of ars){await putEntity('archive_record',r.id,r,{parentId:String(r.archiveSessionId||r.parentId||''),sortTs:r.timestamp||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  for(const r of debtArs){await putEntity('debt_archive_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  for(const c of cats){await putEntity('category',c.id,c,{sortTs:c.id||0,queue:true});total++}
  for(const [k,c] of Object.entries(d.contacts||{})){await putEntity('contact',k,c,{queue:true});total++}
  for(const r of rems){await putEntity('reminder',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.at||0,queue:true});total++}
  for(const r of ledger){await putEntity('ledger_record',r.id,r,{sortTs:r.timestamp||0,queue:true});if(++total%60===0)await new Promise(requestAnimationFrame)}
  if(d.profile){await putEntity('profile','main',profile,{sortTs:Date.now(),queue:true});total++}
  refreshVisibleUI();scheduleAutoSync(300);return{ok:true,total};
}

// ---------------- PWA / storage ----------------
async function requestPersistentStorage(){try{if(navigator.storage?.persist)return await navigator.storage.persist()}catch{}return false}
async function showStorageInfo(){
  try{await requestPersistentStorage();const e=await navigator.storage?.estimate?.();const persisted=await navigator.storage?.persisted?.();const used=Number(e?.usage||0),quota=Number(e?.quota||0);const el=document.getElementById('settings-storage-info');if(el)el.textContent=`${persisted?'دائم':'ديناميكي'} • ${quota?`${formatBytes(used)} من ${formatBytes(quota)}`:formatBytes(used)}`}catch{}
}
function formatBytes(n){if(n<1024)return`${n} B`;if(n<1024**2)return`${(n/1024).toFixed(1)} KB`;if(n<1024**3)return`${(n/1024**2).toFixed(1)} MB`;return`${(n/1024**3).toFixed(2)} GB`}
function updateInstallButton(){const b=document.getElementById('install-app-btn');if(!b)return;const standalone=matchMedia('(display-mode: standalone)').matches||navigator.standalone===true;b.disabled=standalone;b.innerHTML=standalone?'<i class="fas fa-circle-check ml-2"></i>التطبيق مثبت':'<i class="fas fa-download ml-2"></i>تثبيت التطبيق'}
async function chooseInstall(){if(!installPrompt){toast('استخدم قائمة المتصفح ثم «إضافة إلى الشاشة الرئيسية» إذا لم يظهر زر التثبيت.','info');return}installPrompt.prompt();await installPrompt.userChoice;installPrompt=null;updateInstallButton()}

// ---------------- UI/Auth boot ----------------
function setLoginBusy(v){const b=document.getElementById('login-submit');if(b){b.disabled=v;b.innerHTML=v?'<i class="fas fa-circle-notch fa-spin ml-2"></i>جاري الدخول':'<i class="fas fa-right-to-bracket ml-2"></i>دخول الشركة'}}
function showLoginMessage(msg,type='info'){const el=document.getElementById('login-message');if(!el)return;el.textContent=msg;el.className=`login-message ${type}`}
function showLogin(){location.replace('./index.html')}
function hideLogin(){document.getElementById('loginScreen')?.classList.remove('show')}
function bootAuthUI(){
  const reason=localStorage.getItem('cashtop_logout_reason');if(reason){localStorage.removeItem('cashtop_logout_reason');showLoginMessage(reason,'error')}
  if(!session?.token||!session?.company?.id){showLogin();return false}
  hideLogin();document.getElementById('settings-company-name')?.replaceChildren(document.createTextNode(session.company.name||''));document.getElementById('settings-company-expiry')?.replaceChildren(document.createTextNode(new Date(session.company.expiresAt).toLocaleString('ar-EG')));return true
}

function wrapLegacyMutations(){
  // saveData already writes company-scoped localStorage. These wrappers make the cloud queue explicit.
  const oldSavePerson=savePerson;window.savePerson=function(){
    const beforeRecords=new Map(records.map(r=>[String(r.id),JSON.stringify(r)]));
    const beforeContacts=new Map(Object.entries(contacts).map(([k,v])=>[k,JSON.stringify(v)]));
    const beforeReminders=new Map(reminders.map(r=>[String(r.id),JSON.stringify(r)]));
    oldSavePerson();
    setTimeout(async()=>{
      for(const r of records)if(beforeRecords.get(String(r.id))!==JSON.stringify(r))await putEntity('debt_record',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0,queue:true});
      for(const [k,v] of Object.entries(contacts))if(beforeContacts.get(k)!==JSON.stringify(v))await putEntity('contact',k,v,{queue:true});
      for(const k of beforeContacts.keys())if(!(k in contacts))await deleteEntity('contact',k);
      for(const r of reminders)if(beforeReminders.get(String(r.id))!==JSON.stringify(r))await putEntity('reminder',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.at||0,queue:true});
    },0)
  };
  const oldArchivePerson=archivePerson;window.archivePerson=async function(){
    const type=currentDebtTab,name=currentDetailName,doomedRecords=records.filter(r=>r.type===type&&r.name===name).map(r=>({...r})),doomedReminders=reminders.filter(r=>r.type===type&&r.name===name).map(r=>({...r})),contactId=personKey(name,type),hadContact=Object.prototype.hasOwnProperty.call(contacts,contactId);
    await oldArchivePerson();
    if(records.some(r=>r.type===type&&r.name===name))return;
    for(const r of doomedRecords)await deleteEntity('debt_record',r.id,{parentId:personKey(r.name,r.type),sortTs:r.timestamp||0});
    for(const r of doomedReminders)await deleteEntity('reminder',r.id,{parentId:personKey(r.name,r.type),sortTs:r.at||0});
    if(hadContact)await deleteEntity('contact',contactId);
  };
  const oldCat=saveNewCategory;window.saveNewCategory=function(){const n=categories.length;oldCat();if(categories.length>n){const c=categories[categories.length-1];putEntity('category',c.id,c,{sortTs:c.id,queue:true})}};
  const oldReminder=saveDebtReminderForm;window.saveDebtReminderForm=function(){const n=reminders.length;oldReminder();if(reminders.length>n){const r=reminders[reminders.length-1];putEntity('reminder',r.id,r,{parentId:personKey(r.name,r.type),sortTs:r.at,queue:true})}};
  const oldMark=markReminderDone;window.markReminderDone=function(id){oldMark(id);deleteEntity('reminder',id)};
  const oldDelRem=deleteReminder;window.deleteReminder=async function(id){const before=reminders.some(x=>String(x.id)===String(id));await oldDelRem(id);if(before&&!reminders.some(x=>String(x.id)===String(id)))await deleteEntity('reminder',id)};
}

window.addEventListener('beforeinstallprompt',e=>{e.preventDefault();installPrompt=e;updateInstallButton()});
window.addEventListener('appinstalled',()=>{installPrompt=null;updateInstallButton();toast('تم تثبيت التطبيق')});
window.addEventListener('online',()=>{forceNextRemotePull=true;updateSyncUI();validateSession(true);scheduleAutoSync(300)});
window.addEventListener('offline',updateSyncUI);
window.addEventListener('pagehide',()=>{for(const u of localObjectUrls)URL.revokeObjectURL(u);localObjectUrls.clear()});

if('serviceWorker'in navigator&&location.protocol!=='file:')window.addEventListener('load',()=>navigator.serviceWorker.register('./sw.js').catch(console.warn));

document.addEventListener('DOMContentLoaded',async()=>{
  window.loginCompany=()=>login();window.logoutCompany=()=>logout();window.manualSync=()=>syncNow(true);window.installCashTop=()=>chooseInstall();
  const k=document.getElementById('login-company-key');if(k)k.addEventListener('keydown',e=>{if(e.key==='Enter')login()});
  if(!bootAuthUI())return;
  if(isSubscriptionExpired())setTimeout(()=>toast('اشتراكك انتهى','error'),250);
  wrapLegacyMutations();await requestPersistentStorage();await migrateOutboxEconomyV14();await hydrateLegacyFromIndexedDB();await migrateCategoryScopesV10();await migrateLegacy();await updateSyncUI();updateInstallButton();renderArchiveSessions();validateSession(false);scheduleAutoSync(500);
  setInterval(()=>{if(session){validateSession(false);if(navigator.onLine)scheduleAutoSync(100)}},60*1000);
});

})();
