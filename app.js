/* myHistory v0.2.5 — reconstruct & maintain a node's transaction history (summary).
   Data: block.minima.global tRPC (search.get). Storage: MDS H2 SQL. Vanilla JS.
   Reconstructs default-wallet + contract/script addresses. Row-expand shows inputs/outputs only.
   NOTE: on each release bump ALL of: dapp.conf version, VERSION below,
   the app.js?v= cache-bust in index.html, and this header comment. */
(function(){
"use strict";

var VERSION   = "0.2.5";   // displayed version — keep in sync via the 4-spot build checklist
var EXPLORER  = "https://block.minima.global";        // tRPC API host (search.get, address.balance)
var EXPLORER_WEB = "https://explorer.minima.global";  // human-facing explorer — tx page is /transactions/<txid>
var ARCHIVE_RPC = "http://127.0.0.1:16005";
var PER_PAGE  = 1;       // ONE doc per request (~50KB). search.get docs are large (no CORS to
                         // offload, no field-exclude), so the NODE downloads each page — tiny pages keep an
                         // Android/mobile node's heap from OOMing. Slower, but it survives.
var SCAN_THROTTLE = 400; // ms between requests — gives the mobile JVM time to GC
var ADDR_THROTTLE = 400; // ms between addresses
var REFRESH_EVERY = 10;  // re-query/redraw the table only every N addresses during a scan (SELECT is heavy on mobile)
var PAGE_SIZE = 50;      // UI rows per page
var MERGE_CHUNK = 25;    // rows per batched MERGE
var NET_TIMEOUT = 30000; // ms per net.GET
var ARCHIVE_CUTOFF = 1000000;
var SEARCH_DEBOUNCE = 300;    // ms — wait after typing before running the (full-table) SQL search
var MIGRATE_BATCH = 200;      // rows per txdate backfill statement (one-time, for pre-0.1.9 installs)
var ADDR_MAX_TXNS = 20000;    // per-address scan cap — beyond this, scan only the newest N (protects vs huge contracts)
var LOCAL_HISTORY_MAX = 200;  // newest local txns read per resync (MDS history relevant:true) — covers normal deltas; gap-fallback handles more
var DEFAULT_WALLET_SCRIPT_RE = /^\s*RETURN\s+SIGNEDBY\s*\(\s*0x[0-9a-fA-F]+\s*\)\s*$/i;

var state = {
  addrSet:new Set(), addrList:[], page:1, totalRows:0, matchCount:0, openTxid:null,
  sortKey:"txtime", sortDir:-1, busy:false, cancel:false,
  filters:{ dir:{in:true,out:true,self:true}, src:{explorer:true,archive:true}, tok:"", q:"" }
};

/* ---------- MDS helpers ---------- */
function mds(cmd){
  return new Promise(function(res,rej){
    MDS.cmd(cmd,function(r){
      if(!r) return rej(new Error("no response: "+cmd));
      if(r.status===false || r.error) return rej(new Error((r.error||"cmd failed")+" :: "+cmd));
      res(r.response);
    });
  });
}
function sql(q){
  return new Promise(function(res,rej){
    MDS.sql(q,function(r){
      if(!r || r.status===false) return rej(new Error((r&&r.error)||"sql failed"));
      res(r.rows||[]);
    });
  });
}
function netBody(r){
  var b=r&&r.response;
  if(b==null) return null;
  if(typeof b==="object" && b.data!==undefined && b.result===undefined) b=b.data;
  if(typeof b==="string"){ try{ b=JSON.parse(b); }catch(e){ return null; } }
  return b;
}
function netGet(url){
  return new Promise(function(res,rej){
    var done=false;
    var t=setTimeout(function(){ if(!done){ done=true; rej(new Error("timeout")); } }, NET_TIMEOUT);
    MDS.net.GET(url,function(r){
      if(done) return; done=true; clearTimeout(t);
      if(!r || !r.status) return rej(new Error("net.GET failed"));
      var b=netBody(r); if(b==null) return rej(new Error("bad net body"));
      res(b);
    });
  });
}
function trpc(proc,payload){
  var url=EXPLORER+"/api/trpc/"+proc+"?input="+encodeURIComponent(JSON.stringify({json:payload}));
  return netGet(url).then(function(b){
    if(Array.isArray(b)) b=b[0];
    if(b && b.error) throw new Error(b.error.json?b.error.json.message:"trpc error");
    if(b && b.result && b.result.data) return b.result.data.json;
    throw new Error("unexpected trpc shape");
  });
}

/* ---------- SQL helpers ---------- */
function S(v){ return "'"+String(v==null?"":v).replace(/'/g,"''")+"'"; }
function N(v){ var n=Number(v); return isFinite(n)?n:0; }

function createTables(){
  return sql(
    "CREATE TABLE IF NOT EXISTS `txns` ("+
    " `txpowid` VARCHAR(160) NOT NULL,"+
    " `block` BIGINT DEFAULT 0,"+
    " `txtime` BIGINT DEFAULT 0,"+
    " `txdate` VARCHAR(32),"+
    " `direction` VARCHAR(8),"+
    " `in_amount` VARCHAR(80),"+
    " `out_amount` VARCHAR(80),"+
    " `net` VARCHAR(80),"+
    " `tokenids` VARCHAR(400),"+
    " `counterparties` VARCHAR(800),"+
    " `summary_json` TEXT,"+
    " `body_json` TEXT,"+
    " `enriched` INT DEFAULT 0,"+
    " `source` VARCHAR(16),"+
    " PRIMARY KEY (`txpowid`) )"
  ).then(function(){ return sql("CREATE TABLE IF NOT EXISTS `myaddr` (`address` VARCHAR(160), `miniaddress` VARCHAR(160))"); })
   .then(function(){ return sql("CREATE TABLE IF NOT EXISTS `meta` (`k` VARCHAR(64) PRIMARY KEY, `v` VARCHAR(400))"); })
   // signature-key usage per address: one row per (address, root|level1|level3) combo with a use count.
   // total uses for an address = SUM(cnt); reused keys = rows with cnt>=2. Logic ported from MinimaTransactionMonitor.java.
   .then(function(){ return sql(
     "CREATE TABLE IF NOT EXISTS `sigkeys` ("+
     " `address` VARCHAR(160) NOT NULL,"+
     " `keyhash` VARCHAR(440) NOT NULL,"+
     " `rootkey` VARCHAR(80),"+
     " `level1` VARCHAR(80),"+
     " `pubkey` VARCHAR(80),"+
     " `cnt` INT DEFAULT 0,"+
     " PRIMARY KEY (`address`,`keyhash`) )"
   ); })
   // current explorer balance per address (refreshed on each signature scan)
   .then(function(){ return sql("CREATE TABLE IF NOT EXISTS `addrbal` (`address` VARCHAR(160) PRIMARY KEY, `balance` VARCHAR(80))"); })
   // pre-0.1.9 installs: add the txdate column (used for date search) if missing
   .then(function(){ return sql("ALTER TABLE `txns` ADD COLUMN IF NOT EXISTS `txdate` VARCHAR(32)").catch(function(){}); })
   // index the default sort/scan column so paging stays fast on very large histories
   .then(function(){ return sql("CREATE INDEX IF NOT EXISTS `idx_txns_txtime` ON `txns`(`txtime`)").catch(function(){}); });
}
/* one-time backfill of txdate for rows stored before 0.1.9 (so date search covers them).
   Bounded memory: processes MIGRATE_BATCH rows per statement, server-side count, no full load. */
function migrateTxdate(){
  return sql("SELECT COUNT(*) AS C FROM txns WHERE txdate IS NULL").then(function(r){
    var n=r.length?N(r[0].C):0;
    if(!n) return;
    logp("Indexing dates for search ("+n.toLocaleString()+" older rows)…");
    function loop(){
      return sql("SELECT txpowid, txtime FROM txns WHERE txdate IS NULL LIMIT "+MIGRATE_BATCH).then(function(rows){
        if(!rows.length) return;
        var ids=[], cases="";
        rows.forEach(function(row){ var id=S(row.TXPOWID); ids.push(id); cases+=" WHEN "+id+" THEN "+S(fmtDate(N(row.TXTIME))); });
        return sql("UPDATE txns SET txdate = CASE txpowid"+cases+" END WHERE txpowid IN ("+ids.join(",")+")")
          .then(function(){ return delay(20); }).then(loop);
      });
    }
    return loop().then(function(){ logp("Date index complete."); });
  }).catch(function(e){ MDS.log("[myHistory] txdate migrate skipped: "+e.message); });
}
function metaGet(k){ return sql("SELECT v FROM meta WHERE k="+S(k)).then(function(r){ return r.length?r[0].V:null; }); }
function metaSet(k,v){ return sql("MERGE INTO meta (k,v) KEY(k) VALUES ("+S(k)+","+S(v)+")"); }

/* ---------- address enumeration (default wallet only) ---------- */
function enumerateAddresses(){
  return mds("scripts").then(function(scripts){
    scripts=scripts||[];
    var set=new Set(), list=[], seen=new Set();
    scripts.forEach(function(s){
      if(!s || !(s.address||s.miniaddress)) return;
      // DEFAULT WALLET ONLY: tracked contract/newaddress scripts are SHARED (e.g. the FutureCash
      // maximize-stake address everyone's coins sit under) — including them pulls in other people's
      // transactions and mis-attributes them as yours. Skip anything that isn't a seed-derived key.
      if(!DEFAULT_WALLET_SCRIPT_RE.test(s.script||"")) return;
      if(s.address) set.add(String(s.address).toUpperCase());
      if(s.miniaddress) set.add(String(s.miniaddress).toUpperCase());
      var key=s.miniaddress||s.address;
      if(key && !seen.has(key)){ seen.add(key); list.push({address:s.address||"", miniaddress:s.miniaddress||s.address, type:"default"}); }
    });
    state.addrSet=set; state.addrList=list;
    return sql("DELETE FROM myaddr").then(function(){
      if(!list.length) return;
      var vals=list.map(function(a){ return "("+S(a.address)+","+S(a.miniaddress)+")"; }).join(",");
      return sql("INSERT INTO myaddr (address,miniaddress) VALUES "+vals);
    });
  });
}

function addrsFor(scope){
  if(scope==="default") return state.addrList.filter(function(a){ return a.type==="default"; });
  if(scope==="script")  return state.addrList.filter(function(a){ return a.type!=="default"; });
  return state.addrList;
}

/* ---------- classify ---------- */
function isMine(a){ return a && state.addrSet.has(String(a).toUpperCase()); }
/* True only if one of MY default-wallet addresses actually owns a coin (input or output) in this
   tx. Guards against shared-contract txns that merely reference my address in state/proofs, or
   that the node tracks via a shared script address — those are not mine to record. */
function docTouchesMine(doc){
  return (doc.input_addresses||[]).concat(doc.input_mini_addresses||[],
          doc.output_addresses||[], doc.output_mini_addresses||[]).some(isMine);
}
function classify(doc){
  var inA=(doc.input_addresses||[]).concat(doc.input_mini_addresses||[]);
  var outA=(doc.output_addresses||[]).concat(doc.output_mini_addresses||[]);
  var mineIn=inA.some(isMine), mineOut=outA.some(isMine);
  var oa=doc.output_addresses||[], oma=doc.output_mini_addresses||[], amts=doc.output_amounts||[];
  var toMine=0,toOthers=0,cps=[];
  for(var i=0;i<amts.length;i++){
    var mine=isMine(oa[i])||isMine(oma[i]); var amt=N(amts[i]);
    if(mine){ toMine+=amt; } else { toOthers+=amt; var cp=oma[i]||oa[i]; if(cp&&cps.indexOf(cp)<0) cps.push(cp); }
  }
  var dir=mineIn&&mineOut?"self":mineIn?"out":"in";
  var net=dir==="in"?toMine:-toOthers;
  if(dir==="in"&&!cps.length){ (doc.input_mini_addresses||doc.input_addresses||[]).forEach(function(a){ if(!isMine(a)&&cps.indexOf(a)<0) cps.push(a); }); }
  return { direction:dir, in_amount:String(toMine), out_amount:String(toOthers), net:String(net),
           tokenids:(doc.token_ids||[]).join(","), counterparties:cps.slice(0,4).join(",") };
}
/* Reconstruction stores SUMMARY rows only. The per-row expand view fetches inputs/outputs on demand. */
var MERGE_HEAD_SUM ="MERGE INTO txns (txpowid,block,txtime,txdate,direction,in_amount,out_amount,net,tokenids,counterparties,summary_json,source) KEY(txpowid) VALUES ";
function delay(ms){ return new Promise(function(r){ setTimeout(r,ms); }); }
/* compact body for the detail cache — inputs/outputs/state only */
function compactDoc(doc){
  var t=(doc.body||{}).txn||{};
  return { txpow_id:doc.txpow_id, body:{ txn:{inputs:t.inputs, outputs:t.outputs, state:t.state} } };
}
function tupleForDoc(doc){
  var c=classify(doc);
  return "("+S(doc.txpow_id)+","+N(doc.block_number)+","+N(doc.datetime)+","+S(fmtDate(N(doc.datetime)))+","+S(c.direction)+","+
    S(c.in_amount)+","+S(c.out_amount)+","+S(c.net)+","+S(c.tokenids)+","+S(c.counterparties)+",NULL,'explorer')";   // 12-col
}
/* Map a LOCAL node txpow (from `history relevant:true`) into the explorer-document
   shape classify()/tupleForDoc() consume — so the incremental path reuses one code path. */
function docFromLocalTxpow(tp){
  var txn=(tp.body||{}).txn||{}, ins=txn.inputs||[], outs=txn.outputs||[];
  var tids={}; outs.forEach(function(o){ if(o&&o.tokenid) tids[o.tokenid]=1; });
  return {
    txpow_id: tp.txpowid,
    block_number: (tp.header||{}).block,
    datetime: (tp.header||{}).timemilli,
    input_addresses: ins.map(function(i){ return i.address; }),
    input_mini_addresses: ins.map(function(i){ return i.miniaddress; }),
    output_addresses: outs.map(function(o){ return o.address; }),
    output_mini_addresses: outs.map(function(o){ return o.miniaddress; }),
    output_amounts: outs.map(function(o){ return o.amount; }),
    token_ids: Object.keys(tids)
  };
}
function batchMerge(tuples){
  var c2=Promise.resolve();
  for(var i=0;i<tuples.length;i+=MERGE_CHUNK){
    (function(slice){ c2=c2.then(function(){ return sql(MERGE_HEAD_SUM+slice.join(",")); }); })(tuples.slice(i,i+MERGE_CHUNK));
  }
  return c2;
}

/* ---------- progress ---------- */
function logp(m){ var el=document.getElementById("progLog"); el.textContent=(m+"\n"+el.textContent).slice(0,5000); MDS.log("[myHistory] "+m); }
function setProg(p){ document.getElementById("progBar").style.width=Math.max(0,Math.min(100,p))+"%"; }
function showProg(on){ document.getElementById("prog").classList.toggle("hidden",!on); if(on) setProg(0); }
function setBusy(b){
  state.busy=b;
  ["reconBtn","backfillBtn","resyncBtn","sigScanBtn"].forEach(function(id){ var el=document.getElementById(id); if(el) el.disabled=b; });
  document.getElementById("stopBtn").classList.toggle("hidden",!b);
}
function updateButtons(){
  var nd=addrsFor("default").length;
  var rb=document.getElementById("reconBtn"); if(rb) rb.textContent="Reconstruct now"+(nd?(" ("+nd+")"):"");
}
function setVersionDisplay(){
  // Display from the VERSION constant only. We intentionally do NOT call `mds action:list`:
  // `mds` is a privileged management command that lands in the pending-approval queue when the
  // dapp is installed in READ mode. The 4-spot build checklist keeps VERSION in sync, and the
  // app.js?v= cache-bust forces a fresh load after every update, so this never drifts.
  var el=document.getElementById("ver"); el.textContent="v"+VERSION;
}
function endRun(){ setBusy(false); state.cancel=false; }

/* ---------- reconstruction (explorer) ---------- */
function reconstruct(opts){
  opts=opts||{};
  if(state.busy) return Promise.resolve();
  setBusy(true); state.cancel=false; clearStatus(); showProg(true);
  var scope=opts.scope||"all", since=opts.sinceBlock||0, list=addrsFor(scope), total=list.length, done=0, found=0, maxBlock=0;
  if(!total){ endRun(); showStatus("No "+scope+" addresses to scan.","ok"); return Promise.resolve(); }
  logp("Scanning "+total+" "+scope+" address(es)"+(since?(" since block "+since):" (full)")+"…");

  var chain=Promise.resolve();
  if(opts.clean){   // full rebuild: drop prior explorer rows first (keep archive), so removed/foreign txns don't linger
    chain=chain.then(function(){ return sql("DELETE FROM txns WHERE source='explorer'"); })
      .then(function(){ logp("Cleared previous explorer rows for a clean rebuild."); return refresh(); });
  }
  list.forEach(function(a){
    var addr=a.miniaddress||a.address, type=a.type||"";
    chain=chain.then(function(){
      if(state.cancel) return;
      logp("▶ ["+(done+1)+"/"+total+"] "+type+"  "+addr);     // FULL address visible
      var seen0=found;
      return scanAddress(addr, since,
          function(b){ found++; if(b>maxBlock) maxBlock=b; },
          function(pg,tp){ if(tp>1) setProg((done+pg/tp)/total*100); })
        .then(function(){ var n=found-seen0; if(n) logp("    +"+n+" tx"); })
        .catch(function(e){ logp("  ! "+shortAddr(addr)+": "+e.message); });
    }).then(function(){
      done++; setProg(done/total*100);
      var r = (done % REFRESH_EVERY===0) ? refresh() : Promise.resolve();  // refresh table infrequently (heavy on mobile)
      return r.then(function(){ return delay(ADDR_THROTTLE); });
    });
  });

  return chain.then(function(){
    if(maxBlock) return metaGet("last_block").then(function(lb){ return metaSet("last_block", String(Math.max(N(lb),maxBlock))); });
  }).then(function(){ return metaSet("reconstruct_done","1"); })
    .then(function(){ if(opts.clean && !state.cancel) return metaSet("scope_ver","2"); })   // default-wallet-only rebuild done
    .then(function(){ if(maxBlock) return metaSet("tip_at_build",String(maxBlock)); })
    .then(function(){
      logp(state.cancel?("Stopped. "+found+" seen so far."):("Done. "+found+" transaction(s) processed."));
      showStatus(state.cancel?("Stopped — "+found+" processed (re-run to resume)."):("Reconstruction complete — "+found+" transactions."),"ok");
    }).catch(function(e){ showStatus("Reconstruction error: "+e.message+" (re-run to resume).","error"); logp("ERROR "+e.message); })
    .then(function(){ endRun(); return refresh(); });
}
function scanAddress(addr, since, onSeen, onPage){
  var page=1, totalPages=1;
  function nextPage(){
    if(state.cancel) return Promise.resolve();
    return trpc("search.get",{query:addr,filter:"all",page:page,perPage:PER_PAGE}).then(function(r){
      var hits=r.hits||[];
      if(page===1){
        totalPages=Math.max(1,Math.ceil((r.found||0)/PER_PAGE));
        var maxPages=Math.ceil(ADDR_MAX_TXNS/PER_PAGE);
        if(totalPages>maxPages){ logp("    capped: "+(r.found||0)+" txns — scanning newest "+ADDR_MAX_TXNS); totalPages=maxPages; }
      }
      var tuples=[], stop=false;
      hits.forEach(function(h){
        var doc=h.document; if(!doc||!doc.txpow_id) return;
        if(since && N(doc.block_number)<=since){ stop=true; return; }  // sorted desc → reached known
        if(!docTouchesMine(doc)) return;   // address matched only via state/shared contract — not mine
        if(onSeen) onSeen(N(doc.block_number));
        tuples.push(tupleForDoc(doc));
      });
      if(onPage) onPage(page,totalPages);
      return (tuples.length?batchMerge(tuples):Promise.resolve()).then(function(){
        if(stop || state.cancel) return;
        page++; if(page<=totalPages) return delay(SCAN_THROTTLE).then(nextPage);
      });
    });
  }
  return nextPage();
}

/* ---------- signature-key analysis (ported from MinimaTransactionMonitor.java) ----------
   For each input spent from an address we read the witness and pull two public keys:
     Level 1 = the signature whose rootkey matches SIGNEDBY(<rootkey>) in the input's script
     Level 3 = the public key at index 2 of that input's signature group
   We key usage by rootkey|level1|level3 and count how many inputs used each combo.
   "Total uses" for an address = sum of all counts. "Reused" = combos with count >= 2. */
function normHex(h){ if(h==null) return ""; h=String(h); if(h.slice(0,2)==="0x"||h.slice(0,2)==="0X") h=h.slice(2); return h.toUpperCase(); }
function inputIndicesForAddr(doc, addr){
  var idx=[], txn=(doc.body||{}).txn||{}, inputs=txn.inputs||[], A=String(addr).trim();
  for(var i=0;i<inputs.length;i++){ var m=inputs[i]&&inputs[i].miniaddress; if(m!=null && String(m).trim()===A) idx.push(i); }
  return idx;
}
function scriptForInput(witness, i){
  var arr=witness&&witness.scripts; if(!arr||i<0||i>=arr.length) return null;
  var s=arr[i]; return s&&s.script!=null?String(s.script):null;
}
function rootKeyFromScript(script){
  if(!script) return null; var p=script.indexOf("SIGNEDBY("); if(p<0) return null;
  var start=p+9, end=script.indexOf(")",start); if(end<0) return null;
  var rk=script.slice(start,end).trim(); return (rk.slice(0,2)==="0x"||rk.slice(0,2)==="0X")?rk:null;
}
function level1ByRootKey(witness, targetRoot, i){
  var arr=witness&&witness.signatures; if(!arr) return null; var want=normHex(targetRoot);
  function scan(g){ var inner=g&&g.signatures; if(!inner) return null;
    for(var j=0;j<inner.length;j++){ var s=inner[j]; if(s&&s.rootkey!=null&&normHex(s.rootkey)===want&&s.publickey!=null) return normHex(s.publickey); } return null; }
  if(i>=0&&i<arr.length){ var r=scan(arr[i]); if(r) return r; }
  for(var k=0;k<arr.length;k++){ var r2=scan(arr[k]); if(r2) return r2; }
  return null;
}
function level3ByIndex(witness, i){
  var arr=witness&&witness.signatures; if(!arr) return null;
  function pick(g){ var inner=g&&g.signatures; if(inner&&inner.length>2&&inner[2]&&inner[2].publickey!=null) return normHex(inner[2].publickey); return null; }
  if(i>=0&&i<arr.length){ var r=pick(arr[i]); if(r) return r; }
  for(var k=0;k<arr.length;k++){ var r2=pick(arr[k]); if(r2) return r2; }
  return null;
}
/* tally combos for one address into the provided map: keyhash -> {rootkey,level1,pubkey,cnt} */
function tallyDoc(doc, addr, map){
  var idxs=inputIndicesForAddr(doc, addr); if(!idxs.length) return 0;
  var witness=(doc.body||{}).witness; if(!witness) return 0;
  var added=0;
  idxs.forEach(function(i){
    var script=scriptForInput(witness,i); if(!script) return;
    var root=rootKeyFromScript(script); if(!root) return;
    var l1=level1ByRootKey(witness, root, i), l3=level3ByIndex(witness, i);
    if(!l1||!l3) return;
    var rk=normHex(root), key=rk+"|"+l1+"|"+l3;
    var e=map[key]||(map[key]={rootkey:rk,level1:l1,pubkey:l3,cnt:0});
    e.cnt++; added++;
  });
  return added;
}
var SIGKEY_HEAD="MERGE INTO sigkeys (address,keyhash,rootkey,level1,pubkey,cnt) KEY(address,keyhash) VALUES ";
function persistSigkeys(addr, map){
  var rows=Object.keys(map); if(!rows.length) return Promise.resolve();
  var chain=Promise.resolve();
  for(var i=0;i<rows.length;i+=MERGE_CHUNK){
    (function(slice){ chain=chain.then(function(){
      var vals=slice.map(function(k){ var e=map[k];
        return "("+S(addr)+","+S(k)+","+S(e.rootkey)+","+S(e.level1)+","+S(e.pubkey)+","+N(e.cnt)+")"; }).join(",");
      return sql(SIGKEY_HEAD+vals);
    }); })(rows.slice(i,i+MERGE_CHUNK));
  }
  return chain;
}
/* scan witness for one address across all its explorer pages, then write per-combo counts */
function scanAddressSignatures(addr){
  var page=1, totalPages=1, map={};
  function nextPage(){
    if(state.cancel) return Promise.resolve();
    return trpc("search.get",{query:addr,filter:"all",page:page,perPage:PER_PAGE}).then(function(r){
      var hits=r.hits||[];
      if(page===1){
        totalPages=Math.max(1,Math.ceil((r.found||0)/PER_PAGE));
        var maxPages=Math.ceil(ADDR_MAX_TXNS/PER_PAGE);
        if(totalPages>maxPages){ logp("    capped: "+(r.found||0)+" txns — scanning newest "+ADDR_MAX_TXNS); totalPages=maxPages; }
      }
      hits.forEach(function(h){ var doc=h.document; if(doc) tallyDoc(doc, addr, map); });
      if(state.cancel) return;
      page++; if(page<=totalPages) return delay(SCAN_THROTTLE).then(nextPage);
    });
  }
  return nextPage().then(function(){
    return sql("DELETE FROM sigkeys WHERE address="+S(addr)).then(function(){ return persistSigkeys(addr, map); }).then(function(){ return map; });
  });
}
/* current balance of an address from the explorer; stored so the panel can show it without a re-query */
function fetchAddrBalance(addr){
  return trpc("address.balance",{address:addr}).then(function(r){
    var bal=(r&&r.balance!=null)?String(r.balance):"";
    return sql("MERGE INTO addrbal (address,balance) KEY(address) VALUES ("+S(addr)+","+S(bal)+")").then(function(){ return bal; });
  }).catch(function(){ return null; });
}
function scanSignatures(){
  if(state.busy) return Promise.resolve();
  setBusy(true); state.cancel=false; clearStatus(); showProg(true);
  var list=addrsFor("default"), total=list.length, done=0, totalUses=0, reusedKeys=0;
  if(!total){ endRun(); showStatus("No default addresses to scan.","ok"); return Promise.resolve(); }
  logp("Scanning signatures for "+total+" default address(es)…");
  var chain=Promise.resolve();
  list.forEach(function(a){
    var addr=a.miniaddress||a.address, type=a.type||"";
    chain=chain.then(function(){
      if(state.cancel) return;
      logp("▶ ["+(done+1)+"/"+total+"] "+type+"  "+addr);
      return scanAddressSignatures(addr).then(function(map){
        var uses=0, reused=0, mx=0;
        Object.keys(map).forEach(function(k){ var c=map[k].cnt; uses+=c; if(c>=2) reused++; if(c>mx) mx=c; });
        totalUses+=uses; reusedKeys+=reused;
        if(uses) logp("    uses "+uses+", reused keys "+reused+", max "+mx);
        return delay(SCAN_THROTTLE).then(function(){ return fetchAddrBalance(addr); })
          .then(function(bal){ if(bal!=null) logp("    balance "+bal); });
      }).catch(function(e){ logp("  ! "+shortAddr(addr)+": "+e.message); });
    }).then(function(){
      done++; setProg(done/total*100);
      var r=(done%REFRESH_EVERY===0)?renderSigs():Promise.resolve();
      return r.then(function(){ return delay(ADDR_THROTTLE); });
    });
  });
  return chain.then(function(){ return metaSet("sig_scan_done","1"); })
    .then(function(){
      logp(state.cancel?("Stopped. "+totalUses+" uses so far."):("Done. "+totalUses+" signature uses, "+reusedKeys+" reused keys."));
      showStatus(state.cancel?("Signature scan stopped — "+totalUses+" uses (re-run to refresh)."):("Signature scan complete — "+totalUses+" total uses, "+reusedKeys+" reused keys."),"ok");
    }).catch(function(e){ showStatus("Signature scan error: "+e.message,"error"); logp("ERROR "+e.message); })
    .then(function(){ endRun(); return renderSigs(); });
}

/* ---------- archive backfill ---------- */
function backfillArchive(){
  if(state.busy) return Promise.resolve();
  setBusy(true); state.cancel=false; clearStatus(); showProg(true);
  var total=state.addrList.length, done=0, added=0;
  logp("Archive backfill via "+ARCHIVE_RPC+" (coin-level, pre-block "+ARCHIVE_CUTOFF+")…");
  var chain=Promise.resolve();
  state.addrList.forEach(function(a){
    var mx=a.miniaddress||a.address;
    chain=chain.then(function(){
      if(state.cancel) return;
      var url=ARCHIVE_RPC+"/"+encodeURIComponent("archive action:addresscheck address:"+mx);
      return netGet(url).then(function(b){
        var coins=(b&&b.coins)||{}, tuples=[];
        (coins.created||[]).forEach(function(e){ var t=archiveTuple(e,"in"); if(t){ tuples.push(t); added++; } });
        (coins.spent||[]).forEach(function(e){ var t=archiveTuple(e,"out"); if(t){ tuples.push(t); added++; } });
        return batchArchive(tuples);
      }).catch(function(e){ logp("  skip "+shortAddr(mx)+": "+e.message); });
    }).then(function(){ done++; setProg(done/total*100); logp("["+done+"/"+total+"] "+shortAddr(mx)); return refresh(); });
  });
  return chain.then(function(){ return metaSet("archive_done","1"); })
    .then(function(){ showStatus(state.cancel?"Backfill stopped.":("Archive backfill complete — "+added+" pre-2024 coin rows."),"ok"); })
    .catch(function(e){ showStatus("Backfill error: "+e.message,"error"); })
    .then(function(){ endRun(); return refresh(); });
}
function archiveTuple(e,dir){
  var c=e&&e.coin; if(!c) return null;
  var blk=N(e.block); if(blk>=ARCHIVE_CUTOFF) return null;
  var amt=String(c.amount||"0"), id="a:"+dir+":"+(c.coinid||(blk+"_"+amt)), net=dir==="in"?amt:("-"+amt), t=e.datemilli?N(e.datemilli):0;
  return "("+S(id)+","+blk+","+t+","+S(fmtDate(t))+","+S(dir)+","+S(dir==="in"?amt:"0")+","+S(dir==="out"?amt:"0")+","+S(net)+","+
    S(c.tokenid||"0x00")+",''"+","+S(JSON.stringify({archive:true,coin:c,block:blk,date:e.date}))+",'archive')";
}
function batchArchive(tuples){
  var head="MERGE INTO txns (txpowid,block,txtime,txdate,direction,in_amount,out_amount,net,tokenids,counterparties,summary_json,source) KEY(txpowid) VALUES ";
  var chain=Promise.resolve();
  for(var i=0;i<tuples.length;i+=MERGE_CHUNK){ (function(s){ chain=chain.then(function(){ return sql(head+s.join(",")); }); })(tuples.slice(i,i+MERGE_CHUNK)); }
  return chain;
}

/* ---------- incremental ---------- */
var incTimer=null;
function scheduleIncremental(){ if(state.busy) return; clearTimeout(incTimer); incTimer=setTimeout(incremental,1500); }
/* Resync recent — read NEW transactions from the LOCAL node (one MDS.cmd, zero explorer
   round-trips). Falls back to the explorer reconstruct for a fresh install, an over-cap gap,
   or any local error. */
function incremental(){
  if(state.busy) return Promise.resolve();
  return Promise.all([metaGet("last_block"), metaGet("reconstruct_done")]).then(function(m){
    var lb=N(m[0]);
    if(m[1]!=="1") return reconstruct({scope:"all", sinceBlock:lb});   // no baseline yet → full explorer rebuild
    setBusy(true); state.cancel=false; clearStatus();
    return mds("history relevant:true max:"+LOCAL_HISTORY_MAX).then(function(res){
      var txpows=(res&&res.txpows)||[], tuples=[], maxBlock=0, minBlockSeen=Infinity;
      txpows.forEach(function(tp){
        var blk=N((tp.header||{}).block); if(blk<minBlockSeen) minBlockSeen=blk;
        if(!tp.body || !tp.body.txn || blk<=lb) return;       // skip blocks & already-stored txns
        var doc=docFromLocalTxpow(tp);
        if(!docTouchesMine(doc)) return;                      // shared-contract coin the node tracks — not mine
        tuples.push(tupleForDoc(doc));
        if(blk>maxBlock) maxBlock=blk;
      });
      // local window capped before reaching last_block → may have missed older gap txns
      if(txpows.length>=LOCAL_HISTORY_MAX && minBlockSeen>lb+1){
        MDS.log("[myHistory] local window capped → escalating to explorer");
        setBusy(false); return reconstruct({scope:"all", sinceBlock:lb});
      }
      return (tuples.length?batchMerge(tuples):Promise.resolve()).then(function(){
        if(maxBlock>lb) return metaSet("last_block", String(maxBlock));
      }).then(function(){
        if(tuples.length){ MDS.log("[myHistory] +"+tuples.length+" new (local)"); showStatus("Added "+tuples.length+" new transaction(s).","ok"); }
        setBusy(false); return refresh();
      });
    }).catch(function(e){
      MDS.log("[myHistory] local resync failed ("+e.message+") → explorer");
      setBusy(false); return reconstruct({scope:"all", sinceBlock:lb});
    });
  });
}

/* ---------- lazy detail: inputs/outputs (from search.get by txid) ---------- */
function enrich(txpowid, container, row){
  container.innerHTML='<span class="spin"></span>loading detail…';
  return trpc("search.get",{query:txpowid,filter:"all",page:1,perPage:5}).then(function(r){
    var hit=(r.hits||[]).map(function(h){return h.document;}).filter(function(d){ return d && d.txpow_id===txpowid && d.is_transaction; })[0]
          || (r.hits||[]).map(function(h){return h.document;}).filter(function(d){ return d && d.txpow_id===txpowid; })[0];
    if(!hit) throw new Error("transaction not found on explorer");
    var body=JSON.stringify(compactDoc(hit));
    return sql("UPDATE txns SET body_json="+S(body)+", enriched=1 WHERE txpowid="+S(txpowid)).then(function(){
      if(row){ row.BODY_JSON=body; row.ENRICHED=1; }   // so collapse+re-expand uses the cache, no refetch
      renderDetail(container, hit);
    });
  }).catch(function(e){ container.innerHTML='<span style="color:var(--err)">detail unavailable: '+esc(e.message)+'</span>'; });
}
/* Copyable field: full value shown untruncated + a Copy button (mobile-safe). */
function cfield(label,val){
  if(val==null||val==="") return "";
  return '<div class="cf"><span class="cfl">'+esc(label)+'</span>'+
         '<span class="cv">'+esc(val)+'</span>'+
         '<button type="button" class="cpy" title="Copy '+esc(label)+'">⧉</button></div>';
}
function copyText(t){
  t=String(t==null?"":t);
  function fallback(){
    try{ var ta=document.createElement("textarea"); ta.value=t; ta.setAttribute("readonly","");
      ta.style.position="fixed"; ta.style.top="0"; ta.style.opacity="0"; document.body.appendChild(ta);
      ta.focus(); ta.select(); ta.setSelectionRange(0,t.length); document.execCommand("copy");
      document.body.removeChild(ta); }catch(e){}
  }
  try{ if(navigator.clipboard && navigator.clipboard.writeText){ navigator.clipboard.writeText(t).catch(fallback); return; } }catch(e){}
  fallback();
}
/* one delegated handler per detail cell — copies the .cv next to the clicked button */
function wireCopy(container){
  container.addEventListener("click",function(e){
    var b=e.target; if(!b||!b.classList||!b.classList.contains("cpy")) return;
    e.stopPropagation();
    var cv=b.previousElementSibling; if(!cv) return;
    copyText(cv.textContent);
    var o=b.textContent; b.textContent="✓"; b.classList.add("ok");
    setTimeout(function(){ b.textContent=o; b.classList.remove("ok"); },900);
  });
}
function coinBlock(arr,label){
  if(!arr||!arr.length) return "";
  var h='<div class="blk"><h4>'+label+' ('+arr.length+')</h4>';
  arr.forEach(function(c){
    h+='<div class="coin">';
    h+=cfield("address", c.miniaddress||c.address||"");
    if(c.amount!=null) h+=cfield("amount", c.amount);
    if(c.tokenid && c.tokenid!=="0x00") h+=cfield("token", c.tokenid);
    if(c.coinid) h+=cfield("coin", c.coinid);
    h+='</div>';
  });
  return h+'</div>';
}
function renderDetail(container, d){
  if(d.archive){ container.innerHTML='<div class="blk"><h4>Archive coin (coin-level)</h4><pre>'+esc(JSON.stringify(d.coin,null,1))+'</pre></div>'; wireCopy(container); return; }
  var txn=(d.body||{}).txn||{};
  var h="";
  if(d.txpow_id){ h+='<div class="blk"><h4>Transaction</h4>'+cfield("txid", d.txpow_id)+'</div>'; }
  h+=coinBlock(txn.inputs,"Inputs");
  h+=coinBlock(txn.outputs,"Outputs");
  if(txn.state && txn.state.length){ h+='<div class="blk"><h4>State</h4>'+cfield("state", JSON.stringify(txn.state))+'</div>'; }
  if(!txn.inputs && !txn.outputs){ h+='<div class="blk"><pre>no input/output detail available</pre></div>'; }
  h+='<div class="blk"><a href="'+EXPLORER_WEB+'/transactions/'+esc(d.txpow_id)+'" target="_blank" rel="noopener">Open on explorer.minima.global ↗</a></div>';
  container.innerHTML=h;
  wireCopy(container);
}

/* ---------- rendering ---------- */
function fmtDate(ms){ if(!ms) return "—"; return new Date(N(ms)).toISOString().slice(0,19).replace("T"," "); }
function fmtAmt(v){ var n=Number(v); return isFinite(n)?n.toLocaleString(undefined,{maximumFractionDigits:8}):(v||"0"); }
function tokName(t){ if(!t) return "—"; var f=String(t).split(",")[0]; return f==="0x00"?"Minima":(f.slice(0,10)+"…"); }
function shortAddr(a){ a=String(a||""); return a.length>16?(a.slice(0,8)+"…"+a.slice(-4)):a; }
function esc(s){ return String(s==null?"":s).replace(/[&<>"]/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c];}); }

/* Build a SQL WHERE clause from the active filters. Everything is pushed to the DB so we never
   load more than one page (PAGE_SIZE rows) into memory — the whole history stays searchable. */
function whereClause(){
  var f=state.filters, w=[];
  var dirs=["in","out","self"].filter(function(d){ return f.dir[d]; });
  if(dirs.length===0) return "WHERE 1=0";
  if(dirs.length<3) w.push("direction IN ("+dirs.map(S).join(",")+")");
  var srcs=["explorer","archive"].filter(function(s){ return f.src[s]; });
  if(srcs.length===0) return "WHERE 1=0";
  if(srcs.length<2) w.push("source IN ("+srcs.map(S).join(",")+")");
  if(f.tok) w.push("tokenids LIKE "+S("%"+f.tok+"%"));
  if(f.q){
    var like=S("%"+f.q.replace(/[%_\\]/g,"")+"%");   // strip LIKE wildcards; q is already lower-cased
    w.push("(LOWER(txpowid) LIKE "+like+" OR LOWER(counterparties) LIKE "+like+
           " OR LOWER(net) LIKE "+like+" OR LOWER(txdate) LIKE "+like+" OR CAST(block AS VARCHAR) LIKE "+like+")");
  }
  return w.length?("WHERE "+w.join(" AND ")):"";
}
function orderBy(){
  var col={txtime:"txtime",block:"block",net:"CAST(net AS DOUBLE)",direction:"direction",tokenids:"tokenids"}[state.sortKey]||"txtime";
  return " ORDER BY "+col+(state.sortDir<0?" DESC":" ASC");
}
function render(){
  var where=whereClause();
  return sql("SELECT COUNT(*) AS C FROM txns "+where).then(function(cr){
    var count=cr.length?N(cr[0].C):0; state.matchCount=count;
    var pages=Math.max(1,Math.ceil(count/PAGE_SIZE));
    if(state.page>pages) state.page=pages; if(state.page<1) state.page=1;
    var off=(state.page-1)*PAGE_SIZE;
    return sql("SELECT * FROM txns "+where+orderBy()+" LIMIT "+PAGE_SIZE+" OFFSET "+off).then(function(rows){
      var tb=document.getElementById("tbody"); tb.innerHTML="";
      document.getElementById("emptyMsg").classList.toggle("hidden", state.totalRows>0);
      rows.forEach(function(r){
        var tr=document.createElement("tr"); tr.className="tx";
        var dc=r.DIRECTION||"self", net=Number(r.NET)||0, ac=net>0?"in":net<0?"out":"";
        tr.innerHTML=
          '<td data-label="Date" class="mono">'+esc(r.TXDATE||fmtDate(r.TXTIME))+'</td>'+
          '<td data-label="Dir"><span class="pill '+dc+'">'+dc+'</span></td>'+
          '<td data-label="Amount" class="amt '+ac+'">'+(net>0?"+":"")+fmtAmt(r.NET)+'</td>'+
          '<td data-label="Token">'+esc(tokName(r.TOKENIDS))+'</td>'+
          '<td data-label="Counterparty"><span class="trunc mono" title="'+esc(r.COUNTERPARTIES)+'">'+esc(r.COUNTERPARTIES||"—")+'</span></td>'+
          '<td data-label="Block" class="mono">'+(r.BLOCK||"—")+'</td>'+
          '<td data-label="TxID"><span class="trunc mono" title="'+esc(r.TXPOWID)+'">'+esc(r.TXPOWID)+'</span><span class="src">'+esc(r.SOURCE||"")+'</span></td>';
        tr.addEventListener("click",function(){ toggleDetail(tr,r); });
        tb.appendChild(tr);
        if(state.openTxid===r.TXPOWID) openDetailRow(tr,r); // keep an open detail open across re-renders
      });
      renderPager(count,pages);
    });
  }).catch(function(e){ MDS.log("[myHistory] render error: "+e.message); });
}

/* ---------- export (CSV / JSON) ---------- */
function fmtBytes(n){ if(n<1024) return n+" B"; if(n<1048576) return (n/1024).toFixed(1)+" KB"; return (n/1048576).toFixed(2)+" MB"; }
function csvCell(v){ v=String(v==null?"":v); return /[",\n\r]/.test(v)?('"'+v.replace(/"/g,'""')+'"'):v; }
// pull every row matching the current filters, in batches, so a huge history doesn't load in one shot
function exportRows(){
  var where=whereClause(), ob=orderBy(), out=[], off=0, BATCH=1000;
  function loop(){
    return sql("SELECT txpowid,block,txtime,txdate,direction,in_amount,out_amount,net,tokenids,counterparties,source FROM txns "+where+ob+" LIMIT "+BATCH+" OFFSET "+off).then(function(rows){
      out=out.concat(rows);
      if(rows.length<BATCH) return out;
      off+=BATCH; return loop();
    });
  }
  return loop();
}
function doExport(fmt){
  if(state.busy){ showStatus("Busy — let the current scan finish before exporting.","error"); return; }
  if(!state.totalRows){ showStatus("No history to export yet.","error"); return; }
  showStatus("Preparing "+fmt.toUpperCase()+" export…","");
  exportRows().then(function(rows){
    if(!rows.length){ showStatus("Nothing matches the current filters.","error"); return; }
    var cols=[["date","TXDATE"],["block","BLOCK"],["time_ms","TXTIME"],["direction","DIRECTION"],
              ["net","NET"],["in_amount","IN_AMOUNT"],["out_amount","OUT_AMOUNT"],
              ["tokens","TOKENIDS"],["counterparties","COUNTERPARTIES"],["txid","TXPOWID"],["source","SOURCE"]];
    var text, ext, mime;
    if(fmt==="csv"){
      var lines=[cols.map(function(c){return c[0];}).join(",")];
      rows.forEach(function(r){ lines.push(cols.map(function(c){ return csvCell(r[c[1]]); }).join(",")); });
      text=lines.join("\r\n"); ext="csv"; mime="text/csv";
    } else {
      var arr=rows.map(function(r){ return {
        date:r.TXDATE, block:N(r.BLOCK), time_ms:N(r.TXTIME), direction:r.DIRECTION, net:r.NET,
        in_amount:r.IN_AMOUNT, out_amount:r.OUT_AMOUNT,
        tokens:String(r.TOKENIDS||"").split(",").filter(Boolean),
        counterparties:String(r.COUNTERPARTIES||"").split(",").filter(Boolean),
        txid:r.TXPOWID, source:r.SOURCE }; });
      text=JSON.stringify({ generated:new Date().toISOString(), source:EXPLORER, count:arr.length, transactions:arr }, null, 2);
      ext="json"; mime="application/json";
    }
    var name="myhistory_"+new Date().toISOString().replace(/[:.]/g,"-").slice(0,19)+"."+ext;
    try{
      var blob=new Blob([text],{type:mime}), url=URL.createObjectURL(blob), a=document.createElement("a");
      a.href=url; a.download=name; document.body.appendChild(a); a.click();
      setTimeout(function(){ document.body.removeChild(a); URL.revokeObjectURL(url); },1500);
      showStatus("Exported "+rows.length.toLocaleString()+" rows → "+name+" ("+fmtBytes(text.length)+"). If no download appeared, your node's browser may block it.","ok");
    }catch(e){ showStatus("Export failed: "+e.message,"error"); }
  }).catch(function(e){ showStatus("Export error: "+e.message,"error"); });
}
function openDetailRow(tr,r){
  // idempotent: never inserts a second detail row for the same tx
  var nxt=tr.nextElementSibling;
  if(nxt&&nxt.classList.contains("detailrow")) return;
  var dr=document.createElement("tr"); dr.className="detailrow";
  var td=document.createElement("td"); td.className="detail"; td.colSpan=7; dr.appendChild(td);
  tr.parentNode.insertBefore(dr,tr.nextElementSibling);
  if(Number(r.ENRICHED)===1 && r.BODY_JSON){ try{ renderDetail(td, JSON.parse(r.BODY_JSON)); return; }catch(e){} }
  if(String(r.SOURCE)==="archive" && r.SUMMARY_JSON){ try{ renderDetail(td, JSON.parse(r.SUMMARY_JSON)); return; }catch(e){} }
  enrich(r.TXPOWID, td, r);
}
function toggleDetail(tr,r){
  // On touch screens a single tap can fire 'click' twice (ghost click), which would
  // open then immediately close the detail. Ignore a repeat on the same row within 600ms.
  var now=Date.now();
  if(state._tapId===r.TXPOWID && state._tapT && (now-state._tapT)<600) return;
  state._tapT=now; state._tapId=r.TXPOWID;
  var nxt=tr.nextElementSibling;
  if(nxt&&nxt.classList.contains("detailrow")){ nxt.remove(); state.openTxid=null; return; }
  state.openTxid=r.TXPOWID;
  openDetailRow(tr,r);
}
function renderPager(count,pages){
  var p=document.getElementById("pager");
  if(count<=PAGE_SIZE){ p.innerHTML=count?("<span>"+count+" transactions</span>"):""; return; }
  p.innerHTML="";
  function btn(l,pg,dis){ var b=document.createElement("button"); b.className="ghost"; b.textContent=l; b.disabled=dis; b.onclick=function(){ state.page=pg; render(); window.scrollTo(0,0); }; return b; }
  p.appendChild(btn("‹ Prev",state.page-1,state.page<=1));
  var s=document.createElement("span"); s.textContent="Page "+state.page+" / "+pages+"  ("+count+" txns)"; p.appendChild(s);
  p.appendChild(btn("Next ›",state.page+1,state.page>=pages));
}

/* ---------- signatures panel ---------- */
function renderSigs(){
  // Per-address aggregates straight from SQL: total uses = SUM(cnt), reused keys = COUNT(cnt>=2), max reuse = MAX(cnt).
  return sql("SELECT k.address AS ADDRESS, SUM(k.cnt) AS USES, COUNT(*) AS KEYS, "+
             "SUM(CASE WHEN k.cnt>=2 THEN 1 ELSE 0 END) AS REUSED, MAX(k.cnt) AS MAXC, b.balance AS BALANCE "+
             "FROM sigkeys k LEFT JOIN addrbal b ON b.address=k.address GROUP BY k.address, b.balance ORDER BY USES DESC").then(function(rows){
    var tb=document.getElementById("sigBody"); if(!tb) return;
    var totUses=0, totReused=0, totKeys=0;
    rows.forEach(function(r){ totUses+=N(r.USES); totReused+=N(r.REUSED); totKeys+=N(r.KEYS); });
    document.getElementById("sig-uses").textContent=totUses.toLocaleString();
    document.getElementById("sig-reused").textContent=totReused.toLocaleString();
    document.getElementById("sig-keys").textContent=totKeys.toLocaleString();
    document.getElementById("sigEmpty").classList.toggle("hidden", rows.length>0);
    tb.innerHTML="";
    rows.forEach(function(r){
      var addr=r.ADDRESS, reused=N(r.REUSED), maxc=N(r.MAXC);
      var bal=(r.BALANCE!=null&&r.BALANCE!=="")?fmtAmt(r.BALANCE):"—";
      var tr=document.createElement("tr"); tr.className="tx"+(reused?" warn":"");
      var warn=reused?'<span class="sigwarn" title="Key reused — a signing key was used '+maxc+' times">⚠ key reused ×'+maxc+'</span>':'';
      tr.innerHTML=
        '<td data-label="Address"><span class="mono sigaddr">'+esc(addr)+'</span>'+warn+'</td>'+
        '<td data-label="Balance" class="amt">'+bal+'</td>'+
        '<td data-label="Total uses" class="amt">'+N(r.USES).toLocaleString()+'</td>'+
        '<td data-label="Distinct keys">'+N(r.KEYS).toLocaleString()+'</td>'+
        '<td data-label="Reused keys"><span class="'+(reused?"amt out":"")+'">'+reused.toLocaleString()+'</span></td>'+
        '<td data-label="Max reuse">'+maxc.toLocaleString()+'</td>';
      tr.addEventListener("click",function(){ toggleSigDetail(tr, addr); });
      tb.appendChild(tr);
    });
  }).catch(function(e){ MDS.log("[myHistory] renderSigs error: "+e.message); });
}
function toggleSigDetail(tr, addr){
  var nxt=tr.nextElementSibling;
  if(nxt&&nxt.classList.contains("detailrow")){ nxt.remove(); return; }
  var dr=document.createElement("tr"); dr.className="detailrow";
  var td=document.createElement("td"); td.className="detail"; td.colSpan=6; dr.appendChild(td);
  tr.parentNode.insertBefore(dr, tr.nextElementSibling);
  td.innerHTML='<span class="spin"></span>loading keys…';
  // show only reused keys (cnt>=2), the most-used first
  sql("SELECT pubkey,level1,rootkey,cnt FROM sigkeys WHERE address="+S(addr)+" AND cnt>=2 ORDER BY cnt DESC").then(function(rows){
    if(!rows.length){ td.innerHTML='<div class="blk"><pre>no reused keys for this address (every signing key used once)</pre></div>'; return; }
    var h='<div class="blk"><h4>Reused signing keys ('+rows.length+')</h4>';
    rows.forEach(function(r){
      h+='<div class="kv">'+
         '<span class="k">used</span><span class="val">×'+N(r.CNT)+'</span>'+
         '<span class="k">rootkey</span><span class="val">'+esc(r.ROOTKEY)+'</span>'+
         '<span class="k">level 1</span><span class="val">'+esc(r.LEVEL1)+'</span>'+
         '<span class="k">level 3 pubkey</span><span class="val">'+esc(r.PUBKEY)+'</span>'+
         '</div>';
    });
    td.innerHTML=h+'</div>';
  }).catch(function(e){ td.innerHTML='<span style="color:var(--err)">'+esc(e.message)+'</span>'; });
}

/* ---------- status / stats ---------- */
function showStatus(m,k){ var el=document.getElementById("statusBox"); el.textContent=m; el.className="status show "+(k||""); }
function clearStatus(){ var el=document.getElementById("statusBox"); el.className="status"; el.textContent=""; }

function refresh(){
  // Fully SQL-backed: count the whole table, then render only the current page (≤PAGE_SIZE rows).
  // No row cap — memory stays bounded regardless of how large the history grows.
  return sql("SELECT COUNT(*) AS C FROM txns").then(function(cr){
    var total=cr.length?N(cr[0].C):0; state.totalRows=total;
    document.getElementById("s-count").textContent=total.toLocaleString();
    document.getElementById("s-addr").textContent=state.addrList.length;
    updateButtons();
    return populateTokens().then(render).then(updateHeader).then(renderSigs);
  });
}
/* distinct token list for the filter dropdown — derived in SQL (few distinct rows), not from a full load */
function populateTokens(){
  return sql("SELECT DISTINCT tokenids FROM txns").then(function(rows){
    var toks={}; rows.forEach(function(r){ String(r.TOKENIDS||"").split(",").forEach(function(t){ if(t) toks[t]=1; }); });
    var sel=document.getElementById("fTok"), cur=sel.value;
    sel.innerHTML='<option value="">All tokens</option>'+Object.keys(toks).map(function(t){ return '<option value="'+esc(t)+'">'+esc(t==="0x00"?"Minima":t.slice(0,12)+"…")+'</option>'; }).join("");
    sel.value=cur; state.filters.tok=sel.value;
  }).catch(function(){});
}
function updateHeader(){
  return Promise.all([metaGet("last_block"),metaGet("tip_at_build")]).then(function(m){
    document.getElementById("s-tip").textContent=m[0]||m[1]||"—";
    document.getElementById("s-sync").textContent=m[0]?("blk "+m[0]):"never";
  });
}

/* ---------- filters persistence ---------- */
function saveFilters(){ try{ MDS.keypair.set("myhistory_filters", JSON.stringify({dir:state.filters.dir, src:state.filters.src}), function(){}); }catch(e){} }
function loadFilters(){
  return new Promise(function(res){
    try{ MDS.keypair.get("myhistory_filters", function(r){
      if(r && r.status && r.response && r.response.value){ try{ var f=JSON.parse(r.response.value);
        if(f&&f.dir) state.filters.dir=f.dir; if(f&&f.src) state.filters.src=f.src; }catch(e){} }
      res();
    }); }catch(e){ res(); }
  });
}
function syncFilterUI(){
  Array.prototype.forEach.call(document.querySelectorAll("#dirGroup input"),function(c){ c.checked=!!state.filters.dir[c.getAttribute("data-dir")]; });
  Array.prototype.forEach.call(document.querySelectorAll("#srcGroup input"),function(c){ c.checked=!!state.filters.src[c.getAttribute("data-src")]; });
}

/* ---------- init ---------- */
function init(){
  setVersionDisplay();
  document.getElementById("reconBtn").onclick=function(){ reconstruct({scope:"default", clean:true}); };
  document.getElementById("backfillBtn").onclick=function(){ backfillArchive(); };
  document.getElementById("resyncBtn").onclick=function(){ incremental(); };
  document.getElementById("sigScanBtn").onclick=function(){ scanSignatures(); };
  document.getElementById("stopBtn").onclick=function(){ state.cancel=true; logp("Stopping…"); };
  document.getElementById("exportCsvBtn").onclick=function(){ doExport("csv"); };
  document.getElementById("exportJsonBtn").onclick=function(){ doExport("json"); };
  var qTimer=null;
  document.getElementById("q").addEventListener("input",function(){
    var v=this.value.trim().toLowerCase();   // debounce: search scans the whole table, so wait for a typing pause
    clearTimeout(qTimer); qTimer=setTimeout(function(){ state.filters.q=v; state.page=1; render(); }, SEARCH_DEBOUNCE);
  });
  document.getElementById("fTok").addEventListener("change",function(){ state.filters.tok=this.value; state.page=1; render(); });
  Array.prototype.forEach.call(document.querySelectorAll("#dirGroup input"),function(c){
    c.addEventListener("change",function(){ state.filters.dir[c.getAttribute("data-dir")]=c.checked; state.page=1; saveFilters(); render(); }); });
  Array.prototype.forEach.call(document.querySelectorAll("#srcGroup input"),function(c){
    c.addEventListener("change",function(){ state.filters.src[c.getAttribute("data-src")]=c.checked; state.page=1; saveFilters(); render(); }); });
  Array.prototype.forEach.call(document.querySelectorAll("th[data-sort]"),function(th){
    th.addEventListener("click",function(){ var k=th.getAttribute("data-sort"); if(state.sortKey===k) state.sortDir=-state.sortDir; else { state.sortKey=k; state.sortDir=-1; } render(); }); });

  loadFilters()
    .then(syncFilterUI)
    .then(createTables)
    .then(enumerateAddresses)
    .then(refresh)
    .then(function(){ return Promise.all([metaGet("reconstruct_done"), metaGet("scope_ver")]); })
    .then(function(m){
      var done=m[0], scopeVer=m[1];
      if(done==="1" && scopeVer!=="2")   // upgraded to default-wallet-only scope; old DB may hold tracked-contract rows
        showStatus("Updated to default-wallet only. Click Reconstruct now to remove tracked-contract transactions that aren't yours.","");
      else if(done==="1") showStatus("History loaded. New transactions are added automatically.","ok");
      else showStatus("No history yet — click Reconstruct now to rebuild from block.minima.global.","");
      migrateTxdate().then(function(){ if(state.filters.q) render(); });  // backfill old rows' search dates in the background
    })
    .catch(function(e){ showStatus("Init error: "+e.message+" — this dapp must run installed on your node.","error"); });
}

MDS.init(function(msg){
  if(msg.event==="inited"){ MDS.log("myHistory v"+VERSION+" ready"); init(); }
  else if(msg.event==="NEWBALANCE"){ scheduleIncremental(); }
});

})();
