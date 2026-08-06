'use strict';
const VERSION='ct-ready-v17-20260806-invoice-payment-split-1';
const STATIC=VERSION+'-static';
const RUNTIME=VERSION+'-runtime';
const SHELL=['./','./index.html','./app.html','./c.html','./admin.html','./app-logo.png','./download-icon.png','./direct-cloud.js','./cashtop-sync.js','./cashtop-link.js','./ledger.html','./manifest.webmanifest','./icons/icon-192.png','./icons/icon-512.png','./icons/maskable-512.png'];
const REMOTE=[
  'https://cdn.tailwindcss.com/',
  'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
  'https://cdn.jsdelivr.net/npm/sweetalert2@11',
  'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js',
  'https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;800;900&display=swap'
];
self.addEventListener('install',e=>{e.waitUntil((async()=>{const c=await caches.open(STATIC);await c.addAll(SHELL);await Promise.allSettled(REMOTE.map(async url=>{try{const req=new Request(url,{mode:'no-cors',credentials:'omit'}),r=await fetch(req);await c.put(req,r)}catch(_){}}));await self.skipWaiting()})())});
self.addEventListener('activate',e=>{e.waitUntil((async()=>{for(const k of await caches.keys())if(![STATIC,RUNTIME].includes(k))await caches.delete(k);await self.clients.claim()})())});
function cacheable(r){return !!r&&(r.type==='opaque'||(r.ok&&(r.type==='basic'||r.type==='cors')))}
async function cacheFirst(req){const hit=await caches.match(req,{ignoreVary:true});if(hit){fetch(req).then(async r=>{if(cacheable(r)){const c=await caches.open(RUNTIME);await c.put(req,r.clone())}}).catch(()=>{});return hit}try{const r=await fetch(req);if(cacheable(r)){const c=await caches.open(RUNTIME);await c.put(req,r.clone())}return r}catch(e){if(req.mode==='navigate')return(await caches.match('./index.html'))||Response.error();throw e}}
self.addEventListener('fetch',e=>{if(e.request.method!=='GET')return;const u=new URL(e.request.url);if(u.hostname.endsWith('.turso.io')||u.hostname==='storage.bunnycdn.com')return;e.respondWith(cacheFirst(e.request))});
self.addEventListener('message',e=>{if(e.data==='SKIP_WAITING')self.skipWaiting()});
