"use strict";(()=>{var Mt=Object.defineProperty;var Pt=Object.getOwnPropertyDescriptor;var g=(r,t,e,s)=>{for(var i=s>1?void 0:s?Pt(t,e):t,n=r.length-1,o;n>=0;n--)(o=r[n])&&(i=(s?o(t,e,i):o(i))||i);return s&&i&&Mt(t,e,i),i};var L=globalThis,V=L.ShadowRoot&&(L.ShadyCSS===void 0||L.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,ct=Symbol(),lt=new WeakMap,I=class{constructor(t,e,s){if(this._$cssResult$=!0,s!==ct)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=t,this.t=e}get styleSheet(){let t=this.o,e=this.t;if(V&&t===void 0){let s=e!==void 0&&e.length===1;s&&(t=lt.get(e)),t===void 0&&((this.o=t=new CSSStyleSheet).replaceSync(this.cssText),s&&lt.set(e,t))}return t}toString(){return this.cssText}},dt=r=>new I(typeof r=="string"?r:r+"",void 0,ct);var pt=(r,t)=>{if(V)r.adoptedStyleSheets=t.map(e=>e instanceof CSSStyleSheet?e:e.styleSheet);else for(let e of t){let s=document.createElement("style"),i=L.litNonce;i!==void 0&&s.setAttribute("nonce",i),s.textContent=e.cssText,r.appendChild(s)}},J=V?r=>r:r=>r instanceof CSSStyleSheet?(t=>{let e="";for(let s of t.cssRules)e+=s.cssText;return dt(e)})(r):r;var{is:jt,defineProperty:zt,getOwnPropertyDescriptor:Ht,getOwnPropertyNames:Tt,getOwnPropertySymbols:Ut,getPrototypeOf:Nt}=Object,y=globalThis,ht=y.trustedTypes,Lt=ht?ht.emptyScript:"",It=y.reactiveElementPolyfillSupport,k=(r,t)=>r,R={toAttribute(r,t){switch(t){case Boolean:r=r?Lt:null;break;case Object:case Array:r=r==null?r:JSON.stringify(r)}return r},fromAttribute(r,t){let e=r;switch(t){case Boolean:e=r!==null;break;case Number:e=r===null?null:Number(r);break;case Object:case Array:try{e=JSON.parse(r)}catch{e=null}}return e}},W=(r,t)=>!jt(r,t),ut={attribute:!0,type:String,converter:R,reflect:!1,useDefault:!1,hasChanged:W};Symbol.metadata??(Symbol.metadata=Symbol("metadata")),y.litPropertyMetadata??(y.litPropertyMetadata=new WeakMap);var v=class extends HTMLElement{static addInitializer(t){this._$Ei(),(this.l??(this.l=[])).push(t)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(t,e=ut){if(e.state&&(e.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(t)&&((e=Object.create(e)).wrapped=!0),this.elementProperties.set(t,e),!e.noAccessor){let s=Symbol(),i=this.getPropertyDescriptor(t,s,e);i!==void 0&&zt(this.prototype,t,i)}}static getPropertyDescriptor(t,e,s){let{get:i,set:n}=Ht(this.prototype,t)??{get(){return this[e]},set(o){this[e]=o}};return{get:i,set(o){let a=i?.call(this);n?.call(this,o),this.requestUpdate(t,a,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(t){return this.elementProperties.get(t)??ut}static _$Ei(){if(this.hasOwnProperty(k("elementProperties")))return;let t=Nt(this);t.finalize(),t.l!==void 0&&(this.l=[...t.l]),this.elementProperties=new Map(t.elementProperties)}static finalize(){if(this.hasOwnProperty(k("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(k("properties"))){let e=this.properties,s=[...Tt(e),...Ut(e)];for(let i of s)this.createProperty(i,e[i])}let t=this[Symbol.metadata];if(t!==null){let e=litPropertyMetadata.get(t);if(e!==void 0)for(let[s,i]of e)this.elementProperties.set(s,i)}this._$Eh=new Map;for(let[e,s]of this.elementProperties){let i=this._$Eu(e,s);i!==void 0&&this._$Eh.set(i,e)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(t){let e=[];if(Array.isArray(t)){let s=new Set(t.flat(1/0).reverse());for(let i of s)e.unshift(J(i))}else t!==void 0&&e.push(J(t));return e}static _$Eu(t,e){let s=e.attribute;return s===!1?void 0:typeof s=="string"?s:typeof t=="string"?t.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(t=>this.enableUpdating=t),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(t=>t(this))}addController(t){(this._$EO??(this._$EO=new Set)).add(t),this.renderRoot!==void 0&&this.isConnected&&t.hostConnected?.()}removeController(t){this._$EO?.delete(t)}_$E_(){let t=new Map,e=this.constructor.elementProperties;for(let s of e.keys())this.hasOwnProperty(s)&&(t.set(s,this[s]),delete this[s]);t.size>0&&(this._$Ep=t)}createRenderRoot(){let t=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return pt(t,this.constructor.elementStyles),t}connectedCallback(){this.renderRoot??(this.renderRoot=this.createRenderRoot()),this.enableUpdating(!0),this._$EO?.forEach(t=>t.hostConnected?.())}enableUpdating(t){}disconnectedCallback(){this._$EO?.forEach(t=>t.hostDisconnected?.())}attributeChangedCallback(t,e,s){this._$AK(t,s)}_$ET(t,e){let s=this.constructor.elementProperties.get(t),i=this.constructor._$Eu(t,s);if(i!==void 0&&s.reflect===!0){let n=(s.converter?.toAttribute!==void 0?s.converter:R).toAttribute(e,s.type);this._$Em=t,n==null?this.removeAttribute(i):this.setAttribute(i,n),this._$Em=null}}_$AK(t,e){let s=this.constructor,i=s._$Eh.get(t);if(i!==void 0&&this._$Em!==i){let n=s.getPropertyOptions(i),o=typeof n.converter=="function"?{fromAttribute:n.converter}:n.converter?.fromAttribute!==void 0?n.converter:R;this._$Em=i;let a=o.fromAttribute(e,n.type);this[i]=a??this._$Ej?.get(i)??a,this._$Em=null}}requestUpdate(t,e,s,i=!1,n){if(t!==void 0){let o=this.constructor;if(i===!1&&(n=this[t]),s??(s=o.getPropertyOptions(t)),!((s.hasChanged??W)(n,e)||s.useDefault&&s.reflect&&n===this._$Ej?.get(t)&&!this.hasAttribute(o._$Eu(t,s))))return;this.C(t,e,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(t,e,{useDefault:s,reflect:i,wrapped:n},o){s&&!(this._$Ej??(this._$Ej=new Map)).has(t)&&(this._$Ej.set(t,o??e??this[t]),n!==!0||o!==void 0)||(this._$AL.has(t)||(this.hasUpdated||s||(e=void 0),this._$AL.set(t,e)),i===!0&&this._$Em!==t&&(this._$Eq??(this._$Eq=new Set)).add(t))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(e){Promise.reject(e)}let t=this.scheduleUpdate();return t!=null&&await t,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??(this.renderRoot=this.createRenderRoot()),this._$Ep){for(let[i,n]of this._$Ep)this[i]=n;this._$Ep=void 0}let s=this.constructor.elementProperties;if(s.size>0)for(let[i,n]of s){let{wrapped:o}=n,a=this[i];o!==!0||this._$AL.has(i)||a===void 0||this.C(i,void 0,n,a)}}let t=!1,e=this._$AL;try{t=this.shouldUpdate(e),t?(this.willUpdate(e),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(e)):this._$EM()}catch(s){throw t=!1,this._$EM(),s}t&&this._$AE(e)}willUpdate(t){}_$AE(t){this._$EO?.forEach(e=>e.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(t)),this.updated(t)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(t){return!0}update(t){this._$Eq&&(this._$Eq=this._$Eq.forEach(e=>this._$ET(e,this[e]))),this._$EM()}updated(t){}firstUpdated(t){}};v.elementStyles=[],v.shadowRootOptions={mode:"open"},v[k("elementProperties")]=new Map,v[k("finalized")]=new Map,It?.({ReactiveElement:v}),(y.reactiveElementVersions??(y.reactiveElementVersions=[])).push("2.1.2");var P=globalThis,gt=r=>r,D=P.trustedTypes,ft=D?D.createPolicy("lit-html",{createHTML:r=>r}):void 0,xt="$lit$",x=`lit$${Math.random().toFixed(9).slice(2)}$`,St="?"+x,Vt=`<${St}>`,w=document,j=()=>w.createComment(""),z=r=>r===null||typeof r!="object"&&typeof r!="function",tt=Array.isArray,Wt=r=>tt(r)||typeof r?.[Symbol.iterator]=="function",Y=`[ 	
\f\r]`,M=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,mt=/-->/g,bt=/>/g,_=RegExp(`>|${Y}(?:([^\\s"'>=/]+)(${Y}*=${Y}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),vt=/'/g,$t=/"/g,_t=/^(?:script|style|textarea|title)$/i,et=r=>(t,...e)=>({_$litType$:r,strings:t,values:e}),f=et(1),ee=et(2),se=et(3),E=Symbol.for("lit-noChange"),p=Symbol.for("lit-nothing"),yt=new WeakMap,A=w.createTreeWalker(w,129);function At(r,t){if(!tt(r)||!r.hasOwnProperty("raw"))throw Error("invalid template strings array");return ft!==void 0?ft.createHTML(t):t}var Dt=(r,t)=>{let e=r.length-1,s=[],i,n=t===2?"<svg>":t===3?"<math>":"",o=M;for(let a=0;a<e;a++){let l=r[a],d,h,c=-1,b=0;for(;b<l.length&&(o.lastIndex=b,h=o.exec(l),h!==null);)b=o.lastIndex,o===M?h[1]==="!--"?o=mt:h[1]!==void 0?o=bt:h[2]!==void 0?(_t.test(h[2])&&(i=RegExp("</"+h[2],"g")),o=_):h[3]!==void 0&&(o=_):o===_?h[0]===">"?(o=i??M,c=-1):h[1]===void 0?c=-2:(c=o.lastIndex-h[2].length,d=h[1],o=h[3]===void 0?_:h[3]==='"'?$t:vt):o===$t||o===vt?o=_:o===mt||o===bt?o=M:(o=_,i=void 0);let $=o===_&&r[a+1].startsWith("/>")?" ":"";n+=o===M?l+Vt:c>=0?(s.push(d),l.slice(0,c)+xt+l.slice(c)+x+$):l+x+(c===-2?a:$)}return[At(r,n+(r[e]||"<?>")+(t===2?"</svg>":t===3?"</math>":"")),s]},H=class r{constructor({strings:t,_$litType$:e},s){let i;this.parts=[];let n=0,o=0,a=t.length-1,l=this.parts,[d,h]=Dt(t,e);if(this.el=r.createElement(d,s),A.currentNode=this.el.content,e===2||e===3){let c=this.el.content.firstChild;c.replaceWith(...c.childNodes)}for(;(i=A.nextNode())!==null&&l.length<a;){if(i.nodeType===1){if(i.hasAttributes())for(let c of i.getAttributeNames())if(c.endsWith(xt)){let b=h[o++],$=i.getAttribute(c).split(x),N=/([.?@])?(.*)/.exec(b);l.push({type:1,index:n,name:N[2],strings:$,ctor:N[1]==="."?X:N[1]==="?"?G:N[1]==="@"?Z:O}),i.removeAttribute(c)}else c.startsWith(x)&&(l.push({type:6,index:n}),i.removeAttribute(c));if(_t.test(i.tagName)){let c=i.textContent.split(x),b=c.length-1;if(b>0){i.textContent=D?D.emptyScript:"";for(let $=0;$<b;$++)i.append(c[$],j()),A.nextNode(),l.push({type:2,index:++n});i.append(c[b],j())}}}else if(i.nodeType===8)if(i.data===St)l.push({type:2,index:n});else{let c=-1;for(;(c=i.data.indexOf(x,c+1))!==-1;)l.push({type:7,index:n}),c+=x.length-1}n++}}static createElement(t,e){let s=w.createElement("template");return s.innerHTML=t,s}};function C(r,t,e=r,s){if(t===E)return t;let i=s!==void 0?e._$Co?.[s]:e._$Cl,n=z(t)?void 0:t._$litDirective$;return i?.constructor!==n&&(i?._$AO?.(!1),n===void 0?i=void 0:(i=new n(r),i._$AT(r,e,s)),s!==void 0?(e._$Co??(e._$Co=[]))[s]=i:e._$Cl=i),i!==void 0&&(t=C(r,i._$AS(r,t.values),i,s)),t}var K=class{constructor(t,e){this._$AV=[],this._$AN=void 0,this._$AD=t,this._$AM=e}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(t){let{el:{content:e},parts:s}=this._$AD,i=(t?.creationScope??w).importNode(e,!0);A.currentNode=i;let n=A.nextNode(),o=0,a=0,l=s[0];for(;l!==void 0;){if(o===l.index){let d;l.type===2?d=new T(n,n.nextSibling,this,t):l.type===1?d=new l.ctor(n,l.name,l.strings,this,t):l.type===6&&(d=new Q(n,this,t)),this._$AV.push(d),l=s[++a]}o!==l?.index&&(n=A.nextNode(),o++)}return A.currentNode=w,i}p(t){let e=0;for(let s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(t,s,e),e+=s.strings.length-2):s._$AI(t[e])),e++}},T=class r{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(t,e,s,i){this.type=2,this._$AH=p,this._$AN=void 0,this._$AA=t,this._$AB=e,this._$AM=s,this.options=i,this._$Cv=i?.isConnected??!0}get parentNode(){let t=this._$AA.parentNode,e=this._$AM;return e!==void 0&&t?.nodeType===11&&(t=e.parentNode),t}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(t,e=this){t=C(this,t,e),z(t)?t===p||t==null||t===""?(this._$AH!==p&&this._$AR(),this._$AH=p):t!==this._$AH&&t!==E&&this._(t):t._$litType$!==void 0?this.$(t):t.nodeType!==void 0?this.T(t):Wt(t)?this.k(t):this._(t)}O(t){return this._$AA.parentNode.insertBefore(t,this._$AB)}T(t){this._$AH!==t&&(this._$AR(),this._$AH=this.O(t))}_(t){this._$AH!==p&&z(this._$AH)?this._$AA.nextSibling.data=t:this.T(w.createTextNode(t)),this._$AH=t}$(t){let{values:e,_$litType$:s}=t,i=typeof s=="number"?this._$AC(t):(s.el===void 0&&(s.el=H.createElement(At(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===i)this._$AH.p(e);else{let n=new K(i,this),o=n.u(this.options);n.p(e),this.T(o),this._$AH=n}}_$AC(t){let e=yt.get(t.strings);return e===void 0&&yt.set(t.strings,e=new H(t)),e}k(t){tt(this._$AH)||(this._$AH=[],this._$AR());let e=this._$AH,s,i=0;for(let n of t)i===e.length?e.push(s=new r(this.O(j()),this.O(j()),this,this.options)):s=e[i],s._$AI(n),i++;i<e.length&&(this._$AR(s&&s._$AB.nextSibling,i),e.length=i)}_$AR(t=this._$AA.nextSibling,e){for(this._$AP?.(!1,!0,e);t!==this._$AB;){let s=gt(t).nextSibling;gt(t).remove(),t=s}}setConnected(t){this._$AM===void 0&&(this._$Cv=t,this._$AP?.(t))}},O=class{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(t,e,s,i,n){this.type=1,this._$AH=p,this._$AN=void 0,this.element=t,this.name=e,this._$AM=i,this.options=n,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=p}_$AI(t,e=this,s,i){let n=this.strings,o=!1;if(n===void 0)t=C(this,t,e,0),o=!z(t)||t!==this._$AH&&t!==E,o&&(this._$AH=t);else{let a=t,l,d;for(t=n[0],l=0;l<n.length-1;l++)d=C(this,a[s+l],e,l),d===E&&(d=this._$AH[l]),o||(o=!z(d)||d!==this._$AH[l]),d===p?t=p:t!==p&&(t+=(d??"")+n[l+1]),this._$AH[l]=d}o&&!i&&this.j(t)}j(t){t===p?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,t??"")}},X=class extends O{constructor(){super(...arguments),this.type=3}j(t){this.element[this.name]=t===p?void 0:t}},G=class extends O{constructor(){super(...arguments),this.type=4}j(t){this.element.toggleAttribute(this.name,!!t&&t!==p)}},Z=class extends O{constructor(t,e,s,i,n){super(t,e,s,i,n),this.type=5}_$AI(t,e=this){if((t=C(this,t,e,0)??p)===E)return;let s=this._$AH,i=t===p&&s!==p||t.capture!==s.capture||t.once!==s.once||t.passive!==s.passive,n=t!==p&&(s===p||i);i&&this.element.removeEventListener(this.name,this,s),n&&this.element.addEventListener(this.name,this,t),this._$AH=t}handleEvent(t){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,t):this._$AH.handleEvent(t)}},Q=class{constructor(t,e,s){this.element=t,this.type=6,this._$AN=void 0,this._$AM=e,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(t){C(this,t)}};var qt=P.litHtmlPolyfillSupport;qt?.(H,T),(P.litHtmlVersions??(P.litHtmlVersions=[])).push("3.3.2");var wt=(r,t,e)=>{let s=e?.renderBefore??t,i=s._$litPart$;if(i===void 0){let n=e?.renderBefore??null;s._$litPart$=i=new T(t.insertBefore(j(),n),n,void 0,e??{})}return i._$AI(r),i};var U=globalThis,S=class extends v{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){var e;let t=super.createRenderRoot();return(e=this.renderOptions).renderBefore??(e.renderBefore=t.firstChild),t}update(t){let e=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(t),this._$Do=wt(e,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return E}};S._$litElement$=!0,S.finalized=!0,U.litElementHydrateSupport?.({LitElement:S});var Ft=U.litElementPolyfillSupport;Ft?.({LitElement:S});(U.litElementVersions??(U.litElementVersions=[])).push("4.2.2");var Et=r=>(t,e)=>{e!==void 0?e.addInitializer(()=>{customElements.define(r,t)}):customElements.define(r,t)};var Bt={attribute:!0,type:String,converter:R,reflect:!1,hasChanged:W},Jt=(r=Bt,t,e)=>{let{kind:s,metadata:i}=e,n=globalThis.litPropertyMetadata.get(i);if(n===void 0&&globalThis.litPropertyMetadata.set(i,n=new Map),s==="setter"&&((r=Object.create(r)).wrapped=!0),n.set(e.name,r),s==="accessor"){let{name:o}=e;return{set(a){let l=t.get.call(this);t.set.call(this,a),this.requestUpdate(o,l,r,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,r,a),a}}}if(s==="setter"){let{name:o}=e;return function(a){let l=this[o];t.call(this,a),this.requestUpdate(o,l,r,!0,a)}}throw Error("Unsupported decorator location: "+s)};function Ct(r){return(t,e)=>typeof e=="object"?Jt(r,t,e):((s,i,n)=>{let o=i.hasOwnProperty(n);return i.constructor.createProperty(n,s),o?Object.getOwnPropertyDescriptor(i,n):void 0})(r,t,e)}function m(r){return Ct({...r,state:!0,attribute:!1})}var st="ds-inspector-styles";var it="ds-inspector",Ot=`
@keyframes ds-inspector-flash {
  0% { background-color: rgba(250, 204, 21, 0.5); }
  100% { background-color: transparent; }
}

@keyframes ds-inspector-toggle-flash {
  0% {
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4), 0 0 0 0 rgba(250, 204, 21, 0.4);
    border-color: rgba(250, 204, 21, 0.6);
  }
  100% {
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4), 0 0 0 6px rgba(250, 204, 21, 0);
    border-color: rgba(250, 204, 21, 0);
  }
}

.ds-inspector-flash {
  animation: ds-inspector-flash 400ms ease-out;
  border-radius: 2px;
}

.ds-inspector-toggle,
.ds-inspector-panel {
  color-scheme: dark;
  --ds-inspector-bg: #1e1e2e;
  --ds-inspector-surface: #313244;
  --ds-inspector-text: #cdd6f4;
  --ds-inspector-text-dim: #a6adc8;
  --ds-inspector-border: #45475a;
  --ds-inspector-accent: #89b4fa;
  --ds-inspector-key: #f38ba8;
  --ds-inspector-string: #a6e3a1;
  --ds-inspector-number: #fab387;
  --ds-inspector-boolean: #cba6f7;
  --ds-inspector-null: #6c7086;
  --ds-inspector-font: ui-monospace, "SF Mono", "Cascadia Code", "Fira Code", Consolas, monospace;
  --ds-inspector-z-index: 99999;
}

.ds-inspector-toggle {
  position: fixed;
  bottom: 16px;
  right: 16px;
  width: 40px;
  height: 40px;
  border-radius: 8px;
  background: var(--ds-inspector-bg);
  border: 1px solid var(--ds-inspector-border);
  color: var(--ds-inspector-accent);
  font-family: var(--ds-inspector-font);
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: var(--ds-inspector-z-index);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
  transition: transform 0.15s ease, box-shadow 0.15s ease;
}

.ds-inspector-toggle:hover {
  transform: scale(1.05);
  box-shadow: 0 6px 16px rgba(0, 0, 0, 0.5);
}

.ds-inspector-toggle--changed {
  animation: ds-inspector-toggle-flash 400ms ease-out;
}

.ds-inspector-panel {
  position: fixed;
  bottom: 16px;
  right: 16px;
  background: var(--ds-inspector-bg);
  border: 1px solid var(--ds-inspector-border);
  border-radius: 12px;
  display: flex;
  flex-direction: column;
  z-index: var(--ds-inspector-z-index);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
  font-family: var(--ds-inspector-font);
  font-size: 12px;
  color: var(--ds-inspector-text);
}

.ds-inspector-resize-handle {
  position: absolute;
  top: 0;
  left: 0;
  width: 16px;
  height: 16px;
  cursor: nwse-resize;
  border-radius: 12px 0 0 0;
  z-index: 2;
}

.ds-inspector-resize-handle::before {
  content: "";
  position: absolute;
  top: 4px;
  left: 4px;
  width: 6px;
  height: 6px;
  border-left: 2px solid #6c7086;
  border-top: 2px solid #6c7086;
  transition: border-color 0.15s ease;
}

.ds-inspector-resize-handle:hover::before {
  border-color: var(--ds-inspector-accent);
}

.ds-inspector-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-bottom: 1px solid var(--ds-inspector-border);
  background: var(--ds-inspector-surface);
  border-radius: 12px 12px 0 0;
}

.ds-inspector-logo {
  color: var(--ds-inspector-accent);
  font-weight: 700;
  font-size: 11px;
  flex-shrink: 0;
}

.ds-inspector-filter-wrapper {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 4px;
  background: var(--ds-inspector-bg);
  border: 1px solid var(--ds-inspector-border);
  border-radius: 4px;
  padding: 0 6px;
  min-width: 0;
}

.ds-inspector-filter-wrapper:focus-within {
  border-color: var(--ds-inspector-accent);
}

.ds-inspector-filter {
  flex: 1;
  background: transparent;
  border: none;
  color: var(--ds-inspector-text);
  font-family: var(--ds-inspector-font);
  font-size: 11px;
  padding: 4px 0;
  outline: none;
  min-width: 0;
}

.ds-inspector-filter::placeholder { color: var(--ds-inspector-text-dim); }

.ds-inspector-filter-clear,
.ds-inspector-btn {
  background: transparent;
  border: none;
  color: var(--ds-inspector-text-dim);
  cursor: pointer;
  font-family: var(--ds-inspector-font);
}

.ds-inspector-filter-clear {
  padding: 0;
  font-size: 12px;
  line-height: 1;
  flex-shrink: 0;
}

.ds-inspector-filter-clear:hover,
.ds-inspector-btn:hover { color: var(--ds-inspector-text); }

.ds-inspector-view-toggle {
  display: flex;
  background: var(--ds-inspector-bg);
  border: 1px solid var(--ds-inspector-border);
  border-radius: 4px;
  overflow: hidden;
  flex-shrink: 0;
}

.ds-inspector-view-btn {
  padding: 4px 6px;
  border: none;
  background: transparent;
  color: var(--ds-inspector-text-dim);
  font-family: var(--ds-inspector-font);
  font-size: 10px;
  cursor: pointer;
}

.ds-inspector-view-btn:hover { color: var(--ds-inspector-text); }
.ds-inspector-view-btn.active {
  background: var(--ds-inspector-accent);
  color: var(--ds-inspector-bg);
}

.ds-inspector-btn {
  width: 20px;
  height: 20px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  flex-shrink: 0;
}

.ds-inspector-btn:hover { background: var(--ds-inspector-border); }

.ds-inspector-content {
  flex: 1;
  overflow: auto;
  padding: 12px;
  min-height: 0;
}

.ds-inspector-json {
  white-space: pre;
  font-size: 11px;
  line-height: 1.5;
  margin: 0;
}

.ds-inspector-key { color: var(--ds-inspector-key); }
.ds-inspector-string { color: var(--ds-inspector-string); }
.ds-inspector-number { color: var(--ds-inspector-number); }
.ds-inspector-boolean { color: var(--ds-inspector-boolean); }
.ds-inspector-null { color: var(--ds-inspector-null); font-style: italic; }
.ds-inspector-line { border-radius: 2px; }

.ds-inspector-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}

.ds-inspector-table th,
.ds-inspector-table td {
  text-align: left;
  padding: 6px 8px;
  border-bottom: 1px solid var(--ds-inspector-border);
}

.ds-inspector-table th {
  color: var(--ds-inspector-text-dim);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 10px;
  position: sticky;
  top: -12px;
  background: var(--ds-inspector-bg);
  z-index: 1;
}

.ds-inspector-table td:first-child {
  color: var(--ds-inspector-key);
  font-weight: 500;
}

.ds-inspector-table-value {
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ds-inspector-table tr.ds-inspector-flash td {
  animation: ds-inspector-flash 400ms ease-out;
}

.ds-inspector-empty {
  color: var(--ds-inspector-text-dim);
  text-align: center;
  padding: 24px;
  font-style: italic;
}

.ds-inspector-hidden { display: none !important; }

.ds-inspector-content::-webkit-scrollbar { width: 8px; }
.ds-inspector-content::-webkit-scrollbar-track { background: transparent; }
.ds-inspector-content::-webkit-scrollbar-thumb {
  background: var(--ds-inspector-border);
  border-radius: 4px;
}
.ds-inspector-content::-webkit-scrollbar-thumb:hover {
  background: var(--ds-inspector-text-dim);
}
`;function kt(r){return r.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;")}function rt(r){return r.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")}function B(r,t=0){if(typeof r!="object"||r===null)return t+1;for(let e of Object.values(r))t=B(e,t);return t}function nt(r,t=""){let e=[];for(let[s,i]of Object.entries(r)){let n=t?`${t}.${s}`:s;typeof i=="object"&&i!==null&&!Array.isArray(i)?e.push(...nt(i,n)):e.push([n,i])}return e}function Rt(r){if(r.startsWith("/")&&r.lastIndexOf("/")>0){let t=r.lastIndexOf("/"),e=r.slice(1,t),s=r.slice(t+1);try{return new RegExp(e,s||"i")}catch{return new RegExp(rt(r),"i")}}if(r.includes("*")){let t=rt(r).replace(/\\\*/g,".*");return new RegExp(t,"i")}return new RegExp(rt(r),"i")}function ot(r,t,e=""){let s={};for(let[i,n]of Object.entries(r)){let o=e?`${e}.${i}`:i;if(typeof n=="object"&&n!==null&&!Array.isArray(n)){let a=ot(n,t,o);Object.keys(a).length>0&&(s[i]=a)}else(t.test(o)||t.test(String(n)))&&(s[i]=n)}return s}function at(r,t,e=""){let s=new Set;for(let[i,n]of Object.entries(t)){let o=e?`${e}.${i}`:i,a=r[i];typeof n=="object"&&n!==null&&!Array.isArray(n)?typeof a=="object"&&a!==null&&!Array.isArray(a)?at(a,n,o).forEach(d=>s.add(d)):s.add(o):JSON.stringify(a)!==JSON.stringify(n)&&s.add(o)}for(let i of Object.keys(r)){let n=e?`${e}.${i}`:i;i in t||s.add(n)}return s}function F(r,t,e=0,s=""){let i="  ".repeat(e);if(r===null)return'<span class="ds-inspector-null">null</span>';if(typeof r=="boolean")return`<span class="ds-inspector-boolean">${r}</span>`;if(typeof r=="number")return`<span class="ds-inspector-number">${r}</span>`;if(typeof r=="string")return`<span class="ds-inspector-string">"${kt(r)}"</span>`;if(Array.isArray(r))return r.length===0?"[]":`[
${r.map((o,a)=>`${i}  ${F(o,t,e+1,`${s}[${a}]`)}`).join(`,
`)}
${i}]`;if(typeof r=="object"){let n=Object.entries(r);return n.length===0?"{}":`{
${n.map(([a,l])=>{let d=s?`${s}.${a}`:a,h=t.has(d)?" ds-inspector-flash":"",c=`<span class="ds-inspector-key">"${kt(a)}"</span>: ${F(l,t,e+1,d)}`;return`${i}  <span class="ds-inspector-line${h}">${c}</span>`}).join(`,
`)}
${i}}`}return String(r)}var u=class extends S{constructor(){super(...arguments);this.expanded=!1;this.filter="";this.viewMode="json";this.signals={};this.signalCount=0;this.changedPaths=new Set;this.hasUnseenChanges=!1;this.panelWidth=340;this.panelHeight=280;this.observer=null;this.signalsElementId=`ds-inspector-signals-${Math.random().toString(36).slice(2,9)}`;this.previousSignals={};this.flashTimeout=null;this.isResizing=!1;this.resizeStartX=0;this.resizeStartY=0;this.resizeStartWidth=0;this.resizeStartHeight=0;this.minWidth=280;this.maxWidth=600;this.minHeight=200;this.maxHeight=800;this.handleResizeStart=e=>{e.preventDefault(),this.isResizing=!0,this.resizeStartX=e.clientX,this.resizeStartY=e.clientY,this.resizeStartWidth=this.panelWidth,this.resizeStartHeight=this.panelHeight,document.addEventListener("mousemove",this.handleResizeMove),document.addEventListener("mouseup",this.handleResizeEnd)};this.handleResizeMove=e=>{if(!this.isResizing)return;let s=this.resizeStartX-e.clientX,i=this.resizeStartY-e.clientY;this.panelWidth=Math.min(this.maxWidth,Math.max(this.minWidth,this.resizeStartWidth+s)),this.panelHeight=Math.min(this.maxHeight,Math.max(this.minHeight,this.resizeStartHeight+i))};this.handleResizeEnd=()=>{this.isResizing=!1,document.removeEventListener("mousemove",this.handleResizeMove),document.removeEventListener("mouseup",this.handleResizeEnd),this.saveState()}}createRenderRoot(){return this}connectedCallback(){super.connectedCallback(),this.loadState(),this.injectStyles()}disconnectedCallback(){super.disconnectedCallback(),this.observer?.disconnect(),this.flashTimeout&&clearTimeout(this.flashTimeout),document.removeEventListener("mousemove",this.handleResizeMove),document.removeEventListener("mouseup",this.handleResizeEnd)}firstUpdated(){this.setupSignalObserver()}loadState(){try{let e=sessionStorage.getItem(it);if(!e)return;let s=JSON.parse(e);this.expanded=s.expanded??!1,this.filter=s.filter??"",this.viewMode=s.viewMode??"json",this.panelWidth=s.panelWidth??340,this.panelHeight=s.panelHeight??280}catch{}}saveState(){let e={expanded:this.expanded,filter:this.filter,viewMode:this.viewMode,panelWidth:this.panelWidth,panelHeight:this.panelHeight};sessionStorage.setItem(it,JSON.stringify(e))}injectStyles(){if(document.getElementById(st))return;let e=document.createElement("style");e.id=st,e.textContent=Ot,document.head.appendChild(e)}setupSignalObserver(){let e=document.getElementById(this.signalsElementId);e&&(this.parseSignals(e.textContent||"{}",!0),this.observer?.disconnect(),this.observer=new MutationObserver(()=>{this.parseSignals(e.textContent||"{}",!1)}),this.observer.observe(e,{childList:!0,characterData:!0,subtree:!0}))}parseSignals(e,s){try{let i=JSON.parse(e);if(!s&&Object.keys(this.previousSignals).length>0){let n=at(this.previousSignals,i);n.size>0&&(this.changedPaths=n,this.expanded||(this.hasUnseenChanges=!0),this.flashTimeout&&clearTimeout(this.flashTimeout),this.flashTimeout=window.setTimeout(()=>{this.changedPaths=new Set,this.hasUnseenChanges=!1},400))}this.previousSignals=JSON.parse(e),this.signals=i,this.signalCount=B(this.signals)}catch{this.signals={},this.signalCount=0}}getFilteredSignals(){return this.filter.trim()?ot(this.signals,Rt(this.filter.trim())):this.signals}toggle(){this.expanded=!this.expanded,this.saveState(),this.expanded&&(this.hasUnseenChanges=!1,requestAnimationFrame(()=>this.setupSignalObserver()))}close(){this.expanded=!1,this.saveState()}handleFilterInput(e){this.filter=e.target.value,this.saveState()}clearFilter(){this.filter="",this.saveState()}setViewMode(e){this.viewMode=e,this.saveState()}render(){let e=this.getFilteredSignals(),s=B(e),i=this.filter.trim().length>0;return f`
      <pre id="${this.signalsElementId}" class="ds-inspector-hidden" data-json-signals></pre>
      ${this.expanded?this.renderPanel(e,s,i):this.renderToggle()}
    `}renderToggle(){let e=this.hasUnseenChanges?"ds-inspector-toggle ds-inspector-toggle--changed":"ds-inspector-toggle";return f`<button class="${e}" @click=${this.toggle} title="Open Datastar Inspector">DS</button>`}renderPanel(e,s,i){return f`
      <div class="ds-inspector-panel" style="width: ${this.panelWidth}px; height: ${this.panelHeight}px;">
        <div class="ds-inspector-resize-handle" @mousedown=${this.handleResizeStart}></div>
        ${this.renderHeader(s,i)}
        ${this.renderContent(e,i)}
      </div>
    `}renderHeader(e,s){let i=s?`${e}/${this.signalCount} match...`:`Filter ${this.signalCount} signals...`;return f`
      <div class="ds-inspector-header">
        <span class="ds-inspector-logo" title="Datastar Inspector">DS</span>
        <div class="ds-inspector-filter-wrapper">
          <input
            type="text"
            class="ds-inspector-filter"
            placeholder="${i}"
            .value=${this.filter}
            @input=${this.handleFilterInput}
          />
          ${s?f`<button class="ds-inspector-filter-clear" @click=${this.clearFilter}>&times;</button>`:p}
        </div>
        <div class="ds-inspector-view-toggle">
          <button class="ds-inspector-view-btn ${this.viewMode==="json"?"active":""}" @click=${()=>this.setViewMode("json")} title="JSON view">{ }</button>
          <button class="ds-inspector-view-btn ${this.viewMode==="table"?"active":""}" @click=${()=>this.setViewMode("table")} title="Table view">≡</button>
        </div>
        <button class="ds-inspector-btn" @click=${this.close} title="Close">&times;</button>
      </div>
    `}renderContent(e,s){let i=Object.keys(e).length===0;return f`
      <div class="ds-inspector-content">
        ${i?f`<div class="ds-inspector-empty">${s?"No signals match filter":"No signals found"}</div>`:this.viewMode==="json"?this.renderJsonView(e):this.renderTableView(e)}
      </div>
    `}renderJsonView(e){return f`<pre class="ds-inspector-json" .innerHTML=${F(e,this.changedPaths)}></pre>`}renderTableView(e){return f`
      <table class="ds-inspector-table">
        <thead>
          <tr><th>Signal</th><th>Value</th></tr>
        </thead>
        <tbody>
          ${nt(e).map(([s,i])=>f`
            <tr class="${this.changedPaths.has(s)?"ds-inspector-flash":""}">
              <td>${s}</td>
              <td class="ds-inspector-table-value" title=${JSON.stringify(i)}>${JSON.stringify(i)}</td>
            </tr>
          `)}
        </tbody>
      </table>
    `}};g([m()],u.prototype,"expanded",2),g([m()],u.prototype,"filter",2),g([m()],u.prototype,"viewMode",2),g([m()],u.prototype,"signals",2),g([m()],u.prototype,"signalCount",2),g([m()],u.prototype,"changedPaths",2),g([m()],u.prototype,"hasUnseenChanges",2),g([m()],u.prototype,"panelWidth",2),g([m()],u.prototype,"panelHeight",2),u=g([Et("datastar-inspector")],u);})();
/*! Bundled license information:

@lit/reactive-element/css-tag.js:
  (**
   * @license
   * Copyright 2019 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

@lit/reactive-element/reactive-element.js:
lit-html/lit-html.js:
lit-element/lit-element.js:
@lit/reactive-element/decorators/custom-element.js:
@lit/reactive-element/decorators/property.js:
@lit/reactive-element/decorators/state.js:
@lit/reactive-element/decorators/event-options.js:
@lit/reactive-element/decorators/base.js:
@lit/reactive-element/decorators/query.js:
@lit/reactive-element/decorators/query-all.js:
@lit/reactive-element/decorators/query-async.js:
@lit/reactive-element/decorators/query-assigned-nodes.js:
  (**
   * @license
   * Copyright 2017 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

lit-html/is-server.js:
  (**
   * @license
   * Copyright 2022 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

@lit/reactive-element/decorators/query-assigned-elements.js:
  (**
   * @license
   * Copyright 2021 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)
*/
