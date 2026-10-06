const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
for(const page of ['index.html','homebase.html']){
 const html=fs.readFileSync(path.join(__dirname,'..',page),'utf8');
 function setup(){
  const elements={};
  for(const id of ['marmed','mgeofence','moff','controlStatus']){
   const classes=new Set();elements[id]={classList:{toggle:(c,on)=>on?classes.add(c):classes.delete(c),add:c=>classes.add(c),contains:c=>classes.has(c)},disabled:false};
  }
  let resolve;
  const ctx=vm.createContext({$:id=>elements[id],api:()=>new Promise(r=>resolve=r)});
  const buttons=html.slice(html.indexOf('function renderModeButtons('),html.indexOf('async function tasking('));
  const mode=html.slice(html.indexOf('async function mode('),html.indexOf('async function resync('));
  vm.runInContext('let requestedMode=null,modeBusy=false,commandError=null,latestReadiness="ready";'+buttons+'function render(s){renderModeButtons(s)} function renderLive(s){renderModeButtons(s)}'+mode,ctx);
  return {elements,ctx,resolve:r=>resolve(r)};
 }
 for(const selected of ['armed','geofence']){
  test(`${page} ${selected}: immediate yellow, stale refresh safe, accepted pending, confirmed red`,async()=>{
   const t=setup();const running=vm.runInContext(`mode('${selected}')`,t.ctx);
   const b=t.elements['m'+selected];
   assert(b.classList.contains('pending'));assert(!b.classList.contains('confirmed'));
   vm.runInContext(`renderModeButtons({mode:'${selected}',mode_status:'confirmed'})`,t.ctx);
   assert(b.classList.contains('pending'));assert(!b.classList.contains('confirmed'));
   t.resolve({mode:selected,mode_status:'pending'});await running;
   assert(b.classList.contains('pending'));assert(!b.classList.contains('confirmed'));
   vm.runInContext(`renderModeButtons({mode:'${selected}',mode_status:'confirmed',pending_mode:null})`,t.ctx);
   assert(b.classList.contains('confirmed'));assert(!b.classList.contains('pending'));
   const other=selected==='armed'?'geofence':'armed';assert(t.elements['m'+other].classList.contains('inactive'));
  });
 }
 test(`${page}: failed or unconfirmed mode cannot be red`,()=>{
  const t=setup();
  vm.runInContext("renderModeButtons({mode:'armed',mode_status:'confirmed',command_warning:'Failed'})",t.ctx);
  assert(!t.elements.marmed.classList.contains('confirmed'));
 });
 test(`${page}: yellow/red CSS and sensitivity selector removed`,()=>{
  assert.match(html,/(?:\.mode)?\.pending\{[^}]*background:#493b10/);
  assert.match(html,/(?:\.mode)?\.confirmed\{[^}]*background:#6b1f2b/);
  assert(!/onclick="imu\(/.test(html));
  assert(!/id="i(low|medium|high)"/.test(html));
 });
}
