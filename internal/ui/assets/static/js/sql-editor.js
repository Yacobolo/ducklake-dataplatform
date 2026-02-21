(()=>{var vt=Object.defineProperty;var _t=(n,t,e)=>t in n?vt(n,t,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[t]=e;var u=(n,t,e)=>_t(n,typeof t!="symbol"?t+"":t,e);var R=globalThis,I=R.ShadowRoot&&(R.ShadyCSS===void 0||R.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,B=Symbol(),Q=new WeakMap,C=class{constructor(t,e,s){if(this._$cssResult$=!0,s!==B)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=t,this.t=e}get styleSheet(){let t=this.o,e=this.t;if(I&&t===void 0){let s=e!==void 0&&e.length===1;s&&(t=Q.get(e)),t===void 0&&((this.o=t=new CSSStyleSheet).replaceSync(this.cssText),s&&Q.set(e,t))}return t}toString(){return this.cssText}},X=n=>new C(typeof n=="string"?n:n+"",void 0,B),D=(n,...t)=>{let e=n.length===1?n[0]:t.reduce((s,i,r)=>s+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(i)+n[r+1],n[0]);return new C(e,n,B)},tt=(n,t)=>{if(I)n.adoptedStyleSheets=t.map(e=>e instanceof CSSStyleSheet?e:e.styleSheet);else for(let e of t){let s=document.createElement("style"),i=R.litNonce;i!==void 0&&s.setAttribute("nonce",i),s.textContent=e.cssText,n.appendChild(s)}},j=I?n=>n:n=>n instanceof CSSStyleSheet?(t=>{let e="";for(let s of t.cssRules)e+=s.cssText;return X(e)})(n):n;var{is:gt,defineProperty:At,getOwnPropertyDescriptor:yt,getOwnPropertyNames:Et,getOwnPropertySymbols:St,getPrototypeOf:bt}=Object,v=globalThis,et=v.trustedTypes,xt=et?et.emptyScript:"",Ct=v.reactiveElementPolyfillSupport,w=(n,t)=>n,q={toAttribute(n,t){switch(t){case Boolean:n=n?xt:null;break;case Object:case Array:n=n==null?n:JSON.stringify(n)}return n},fromAttribute(n,t){let e=n;switch(t){case Boolean:e=n!==null;break;case Number:e=n===null?null:Number(n);break;case Object:case Array:try{e=JSON.parse(n)}catch{e=null}}return e}},it=(n,t)=>!gt(n,t),st={attribute:!0,type:String,converter:q,reflect:!1,useDefault:!1,hasChanged:it};Symbol.metadata??(Symbol.metadata=Symbol("metadata")),v.litPropertyMetadata??(v.litPropertyMetadata=new WeakMap);var m=class extends HTMLElement{static addInitializer(t){this._$Ei(),(this.l??(this.l=[])).push(t)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(t,e=st){if(e.state&&(e.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(t)&&((e=Object.create(e)).wrapped=!0),this.elementProperties.set(t,e),!e.noAccessor){let s=Symbol(),i=this.getPropertyDescriptor(t,s,e);i!==void 0&&At(this.prototype,t,i)}}static getPropertyDescriptor(t,e,s){let{get:i,set:r}=yt(this.prototype,t)??{get(){return this[e]},set(o){this[e]=o}};return{get:i,set(o){let l=i?.call(this);r?.call(this,o),this.requestUpdate(t,l,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(t){return this.elementProperties.get(t)??st}static _$Ei(){if(this.hasOwnProperty(w("elementProperties")))return;let t=bt(this);t.finalize(),t.l!==void 0&&(this.l=[...t.l]),this.elementProperties=new Map(t.elementProperties)}static finalize(){if(this.hasOwnProperty(w("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(w("properties"))){let e=this.properties,s=[...Et(e),...St(e)];for(let i of s)this.createProperty(i,e[i])}let t=this[Symbol.metadata];if(t!==null){let e=litPropertyMetadata.get(t);if(e!==void 0)for(let[s,i]of e)this.elementProperties.set(s,i)}this._$Eh=new Map;for(let[e,s]of this.elementProperties){let i=this._$Eu(e,s);i!==void 0&&this._$Eh.set(i,e)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(t){let e=[];if(Array.isArray(t)){let s=new Set(t.flat(1/0).reverse());for(let i of s)e.unshift(j(i))}else t!==void 0&&e.push(j(t));return e}static _$Eu(t,e){let s=e.attribute;return s===!1?void 0:typeof s=="string"?s:typeof t=="string"?t.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(t=>this.enableUpdating=t),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(t=>t(this))}addController(t){(this._$EO??(this._$EO=new Set)).add(t),this.renderRoot!==void 0&&this.isConnected&&t.hostConnected?.()}removeController(t){this._$EO?.delete(t)}_$E_(){let t=new Map,e=this.constructor.elementProperties;for(let s of e.keys())this.hasOwnProperty(s)&&(t.set(s,this[s]),delete this[s]);t.size>0&&(this._$Ep=t)}createRenderRoot(){let t=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return tt(t,this.constructor.elementStyles),t}connectedCallback(){this.renderRoot??(this.renderRoot=this.createRenderRoot()),this.enableUpdating(!0),this._$EO?.forEach(t=>t.hostConnected?.())}enableUpdating(t){}disconnectedCallback(){this._$EO?.forEach(t=>t.hostDisconnected?.())}attributeChangedCallback(t,e,s){this._$AK(t,s)}_$ET(t,e){let s=this.constructor.elementProperties.get(t),i=this.constructor._$Eu(t,s);if(i!==void 0&&s.reflect===!0){let r=(s.converter?.toAttribute!==void 0?s.converter:q).toAttribute(e,s.type);this._$Em=t,r==null?this.removeAttribute(i):this.setAttribute(i,r),this._$Em=null}}_$AK(t,e){let s=this.constructor,i=s._$Eh.get(t);if(i!==void 0&&this._$Em!==i){let r=s.getPropertyOptions(i),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:q;this._$Em=i;let l=o.fromAttribute(e,r.type);this[i]=l??this._$Ej?.get(i)??l,this._$Em=null}}requestUpdate(t,e,s,i=!1,r){if(t!==void 0){let o=this.constructor;if(i===!1&&(r=this[t]),s??(s=o.getPropertyOptions(t)),!((s.hasChanged??it)(r,e)||s.useDefault&&s.reflect&&r===this._$Ej?.get(t)&&!this.hasAttribute(o._$Eu(t,s))))return;this.C(t,e,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(t,e,{useDefault:s,reflect:i,wrapped:r},o){s&&!(this._$Ej??(this._$Ej=new Map)).has(t)&&(this._$Ej.set(t,o??e??this[t]),r!==!0||o!==void 0)||(this._$AL.has(t)||(this.hasUpdated||s||(e=void 0),this._$AL.set(t,e)),i===!0&&this._$Em!==t&&(this._$Eq??(this._$Eq=new Set)).add(t))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(e){Promise.reject(e)}let t=this.scheduleUpdate();return t!=null&&await t,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??(this.renderRoot=this.createRenderRoot()),this._$Ep){for(let[i,r]of this._$Ep)this[i]=r;this._$Ep=void 0}let s=this.constructor.elementProperties;if(s.size>0)for(let[i,r]of s){let{wrapped:o}=r,l=this[i];o!==!0||this._$AL.has(i)||l===void 0||this.C(i,void 0,r,l)}}let t=!1,e=this._$AL;try{t=this.shouldUpdate(e),t?(this.willUpdate(e),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(e)):this._$EM()}catch(s){throw t=!1,this._$EM(),s}t&&this._$AE(e)}willUpdate(t){}_$AE(t){this._$EO?.forEach(e=>e.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(t)),this.updated(t)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(t){return!0}update(t){this._$Eq&&(this._$Eq=this._$Eq.forEach(e=>this._$ET(e,this[e]))),this._$EM()}updated(t){}firstUpdated(t){}};m.elementStyles=[],m.shadowRootOptions={mode:"open"},m[w("elementProperties")]=new Map,m[w("finalized")]=new Map,Ct?.({ReactiveElement:m}),(v.reactiveElementVersions??(v.reactiveElementVersions=[])).push("2.1.2");var H=globalThis,nt=n=>n,z=H.trustedTypes,rt=z?z.createPolicy("lit-html",{createHTML:n=>n}):void 0,dt="$lit$",_=`lit$${Math.random().toFixed(9).slice(2)}$`,pt="?"+_,wt=`<${pt}>`,E=document,P=()=>E.createComment(""),L=n=>n===null||typeof n!="object"&&typeof n!="function",Z=Array.isArray,Tt=n=>Z(n)||typeof n?.[Symbol.iterator]=="function",V=`[ 	
\f\r]`,T=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,ot=/-->/g,at=/>/g,A=RegExp(`>|${V}(?:([^\\s"'>=/]+)(${V}*=${V}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),ht=/'/g,lt=/"/g,ut=/^(?:script|style|textarea|title)$/i,G=n=>(t,...e)=>({_$litType$:n,strings:t,values:e}),$t=G(1),Rt=G(2),It=G(3),S=Symbol.for("lit-noChange"),d=Symbol.for("lit-nothing"),ct=new WeakMap,y=E.createTreeWalker(E,129);function mt(n,t){if(!Z(n)||!n.hasOwnProperty("raw"))throw Error("invalid template strings array");return rt!==void 0?rt.createHTML(t):t}var Ht=(n,t)=>{let e=n.length-1,s=[],i,r=t===2?"<svg>":t===3?"<math>":"",o=T;for(let l=0;l<e;l++){let a=n[l],c,p,h=-1,$=0;for(;$<a.length&&(o.lastIndex=$,p=o.exec(a),p!==null);)$=o.lastIndex,o===T?p[1]==="!--"?o=ot:p[1]!==void 0?o=at:p[2]!==void 0?(ut.test(p[2])&&(i=RegExp("</"+p[2],"g")),o=A):p[3]!==void 0&&(o=A):o===A?p[0]===">"?(o=i??T,h=-1):p[1]===void 0?h=-2:(h=o.lastIndex-p[2].length,c=p[1],o=p[3]===void 0?A:p[3]==='"'?lt:ht):o===lt||o===ht?o=A:o===ot||o===at?o=T:(o=A,i=void 0);let f=o===A&&n[l+1].startsWith("/>")?" ":"";r+=o===T?a+wt:h>=0?(s.push(c),a.slice(0,h)+dt+a.slice(h)+_+f):a+_+(h===-2?l:f)}return[mt(n,r+(n[e]||"<?>")+(t===2?"</svg>":t===3?"</math>":"")),s]},M=class n{constructor({strings:t,_$litType$:e},s){let i;this.parts=[];let r=0,o=0,l=t.length-1,a=this.parts,[c,p]=Ht(t,e);if(this.el=n.createElement(c,s),y.currentNode=this.el.content,e===2||e===3){let h=this.el.content.firstChild;h.replaceWith(...h.childNodes)}for(;(i=y.nextNode())!==null&&a.length<l;){if(i.nodeType===1){if(i.hasAttributes())for(let h of i.getAttributeNames())if(h.endsWith(dt)){let $=p[o++],f=i.getAttribute(h).split(_),O=/([.?@])?(.*)/.exec($);a.push({type:1,index:r,name:O[2],strings:f,ctor:O[1]==="."?F:O[1]==="?"?K:O[1]==="@"?J:x}),i.removeAttribute(h)}else h.startsWith(_)&&(a.push({type:6,index:r}),i.removeAttribute(h));if(ut.test(i.tagName)){let h=i.textContent.split(_),$=h.length-1;if($>0){i.textContent=z?z.emptyScript:"";for(let f=0;f<$;f++)i.append(h[f],P()),y.nextNode(),a.push({type:2,index:++r});i.append(h[$],P())}}}else if(i.nodeType===8)if(i.data===pt)a.push({type:2,index:r});else{let h=-1;for(;(h=i.data.indexOf(_,h+1))!==-1;)a.push({type:7,index:r}),h+=_.length-1}r++}}static createElement(t,e){let s=E.createElement("template");return s.innerHTML=t,s}};function b(n,t,e=n,s){if(t===S)return t;let i=s!==void 0?e._$Co?.[s]:e._$Cl,r=L(t)?void 0:t._$litDirective$;return i?.constructor!==r&&(i?._$AO?.(!1),r===void 0?i=void 0:(i=new r(n),i._$AT(n,e,s)),s!==void 0?(e._$Co??(e._$Co=[]))[s]=i:e._$Cl=i),i!==void 0&&(t=b(n,i._$AS(n,t.values),i,s)),t}var W=class{constructor(t,e){this._$AV=[],this._$AN=void 0,this._$AD=t,this._$AM=e}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(t){let{el:{content:e},parts:s}=this._$AD,i=(t?.creationScope??E).importNode(e,!0);y.currentNode=i;let r=y.nextNode(),o=0,l=0,a=s[0];for(;a!==void 0;){if(o===a.index){let c;a.type===2?c=new U(r,r.nextSibling,this,t):a.type===1?c=new a.ctor(r,a.name,a.strings,this,t):a.type===6&&(c=new Y(r,this,t)),this._$AV.push(c),a=s[++l]}o!==a?.index&&(r=y.nextNode(),o++)}return y.currentNode=E,i}p(t){let e=0;for(let s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(t,s,e),e+=s.strings.length-2):s._$AI(t[e])),e++}},U=class n{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(t,e,s,i){this.type=2,this._$AH=d,this._$AN=void 0,this._$AA=t,this._$AB=e,this._$AM=s,this.options=i,this._$Cv=i?.isConnected??!0}get parentNode(){let t=this._$AA.parentNode,e=this._$AM;return e!==void 0&&t?.nodeType===11&&(t=e.parentNode),t}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(t,e=this){t=b(this,t,e),L(t)?t===d||t==null||t===""?(this._$AH!==d&&this._$AR(),this._$AH=d):t!==this._$AH&&t!==S&&this._(t):t._$litType$!==void 0?this.$(t):t.nodeType!==void 0?this.T(t):Tt(t)?this.k(t):this._(t)}O(t){return this._$AA.parentNode.insertBefore(t,this._$AB)}T(t){this._$AH!==t&&(this._$AR(),this._$AH=this.O(t))}_(t){this._$AH!==d&&L(this._$AH)?this._$AA.nextSibling.data=t:this.T(E.createTextNode(t)),this._$AH=t}$(t){let{values:e,_$litType$:s}=t,i=typeof s=="number"?this._$AC(t):(s.el===void 0&&(s.el=M.createElement(mt(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===i)this._$AH.p(e);else{let r=new W(i,this),o=r.u(this.options);r.p(e),this.T(o),this._$AH=r}}_$AC(t){let e=ct.get(t.strings);return e===void 0&&ct.set(t.strings,e=new M(t)),e}k(t){Z(this._$AH)||(this._$AH=[],this._$AR());let e=this._$AH,s,i=0;for(let r of t)i===e.length?e.push(s=new n(this.O(P()),this.O(P()),this,this.options)):s=e[i],s._$AI(r),i++;i<e.length&&(this._$AR(s&&s._$AB.nextSibling,i),e.length=i)}_$AR(t=this._$AA.nextSibling,e){for(this._$AP?.(!1,!0,e);t!==this._$AB;){let s=nt(t).nextSibling;nt(t).remove(),t=s}}setConnected(t){this._$AM===void 0&&(this._$Cv=t,this._$AP?.(t))}},x=class{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(t,e,s,i,r){this.type=1,this._$AH=d,this._$AN=void 0,this.element=t,this.name=e,this._$AM=i,this.options=r,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=d}_$AI(t,e=this,s,i){let r=this.strings,o=!1;if(r===void 0)t=b(this,t,e,0),o=!L(t)||t!==this._$AH&&t!==S,o&&(this._$AH=t);else{let l=t,a,c;for(t=r[0],a=0;a<r.length-1;a++)c=b(this,l[s+a],e,a),c===S&&(c=this._$AH[a]),o||(o=!L(c)||c!==this._$AH[a]),c===d?t=d:t!==d&&(t+=(c??"")+r[a+1]),this._$AH[a]=c}o&&!i&&this.j(t)}j(t){t===d?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,t??"")}},F=class extends x{constructor(){super(...arguments),this.type=3}j(t){this.element[this.name]=t===d?void 0:t}},K=class extends x{constructor(){super(...arguments),this.type=4}j(t){this.element.toggleAttribute(this.name,!!t&&t!==d)}},J=class extends x{constructor(t,e,s,i,r){super(t,e,s,i,r),this.type=5}_$AI(t,e=this){if((t=b(this,t,e,0)??d)===S)return;let s=this._$AH,i=t===d&&s!==d||t.capture!==s.capture||t.once!==s.once||t.passive!==s.passive,r=t!==d&&(s===d||i);i&&this.element.removeEventListener(this.name,this,s),r&&this.element.addEventListener(this.name,this,t),this._$AH=t}handleEvent(t){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,t):this._$AH.handleEvent(t)}},Y=class{constructor(t,e,s){this.element=t,this.type=6,this._$AN=void 0,this._$AM=e,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(t){b(this,t)}};var Pt=H.litHtmlPolyfillSupport;Pt?.(M,U),(H.litHtmlVersions??(H.litHtmlVersions=[])).push("3.3.2");var ft=(n,t,e)=>{let s=e?.renderBefore??t,i=s._$litPart$;if(i===void 0){let r=e?.renderBefore??null;s._$litPart$=i=new U(t.insertBefore(P(),r),r,void 0,e??{})}return i._$AI(n),i};var N=globalThis,g=class extends m{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){var e;let t=super.createRenderRoot();return(e=this.renderOptions).renderBefore??(e.renderBefore=t.firstChild),t}update(t){let e=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(t),this._$Do=ft(e,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return S}};g._$litElement$=!0,g.finalized=!0,N.litElementHydrateSupport?.({LitElement:g});var Lt=N.litElementPolyfillSupport;Lt?.({LitElement:g});(N.litElementVersions??(N.litElementVersions=[])).push("4.2.2");var k=class extends g{constructor(){super(...arguments);u(this,"lineCount",16);u(this,"activeLine",1);u(this,"lineHeight",20);u(this,"activeTop",0);u(this,"textarea",null);u(this,"onSlotChange",e=>{let s=e.target;s instanceof HTMLSlotElement&&this.attachTextarea(s)});u(this,"onInput",()=>{this.syncState()})}connectedCallback(){super.connectedCallback(),window.addEventListener("resize",this.onInput)}disconnectedCallback(){window.removeEventListener("resize",this.onInput),this.detachTextarea(),super.disconnectedCallback()}firstUpdated(){let e=this.renderRoot.querySelector("slot");e instanceof HTMLSlotElement&&(e.addEventListener("slotchange",this.onSlotChange),this.attachTextarea(e))}render(){let e=[];for(let s=1;s<=this.lineCount;s+=1)e.push(s);return $t`
      <pre class="gutter" aria-hidden="true">${e.join(`
`)}</pre>
      <div class="input-wrap">
        <div
          class="active-line"
          aria-hidden="true"
          style=${`height: ${this.lineHeight}px; transform: translateY(${this.activeTop}px);`}
        ></div>
        <slot></slot>
      </div>
    `}attachTextarea(e){let i=e.assignedElements({flatten:!0}).find(r=>r instanceof HTMLTextAreaElement);if(!(i instanceof HTMLTextAreaElement)){this.detachTextarea();return}if(this.textarea===i){this.syncState();return}this.detachTextarea(),this.textarea=i,i.addEventListener("input",this.onInput),i.addEventListener("scroll",this.onInput),i.addEventListener("keyup",this.onInput),i.addEventListener("click",this.onInput),i.addEventListener("select",this.onInput),this.syncState()}detachTextarea(){if(!(this.textarea instanceof HTMLTextAreaElement)){this.textarea=null;return}this.textarea.removeEventListener("input",this.onInput),this.textarea.removeEventListener("scroll",this.onInput),this.textarea.removeEventListener("keyup",this.onInput),this.textarea.removeEventListener("click",this.onInput),this.textarea.removeEventListener("select",this.onInput),this.textarea=null}syncState(){if(!(this.textarea instanceof HTMLTextAreaElement))return;let e=this.textarea.value||"",s=e.split(`
`).length;this.lineCount=Math.max(16,s+2);let i=window.getComputedStyle(this.textarea),r=Number.parseFloat(i.lineHeight);this.lineHeight=Number.isFinite(r)&&r>0?r:20;let o=Number.parseFloat(i.paddingTop),l=Number.isFinite(o)?o:0,a=this.textarea.selectionStart||0;this.activeLine=e.slice(0,a).split(`
`).length,this.activeTop=l+(this.activeLine-1)*this.lineHeight-this.textarea.scrollTop}};u(k,"properties",{lineCount:{state:!0},activeLine:{state:!0},lineHeight:{state:!0},activeTop:{state:!0}}),u(k,"styles",D`
    :host {
      position: relative;
      display: grid;
      grid-template-columns: calc(var(--control-small-size) + var(--space-3)) minmax(0, 1fr);
      min-height: 0;
      flex: 1;
      background: linear-gradient(180deg, var(--bgColor-default), var(--bgColor-muted));
    }

    .gutter {
      margin: 0;
      padding: var(--space-2) var(--space-1) var(--space-2) var(--space-2);
      border-right: var(--borderWidth-default) solid var(--borderColor-muted);
      color: var(--fgColor-muted);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      text-align: right;
      white-space: pre;
      user-select: none;
      overflow: hidden;
    }

    .input-wrap {
      position: relative;
      min-height: 0;
      overflow: hidden;
    }

    .active-line {
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      background: var(--bgColor-accent-muted);
      opacity: 0;
      pointer-events: none;
      transition: opacity var(--base-duration-100) ease;
      z-index: 1;
    }

    .input-wrap:focus-within .active-line {
      opacity: 1;
    }

    ::slotted(textarea.sql-editor-textarea) {
      margin: 0;
      border: 0;
      border-radius: 0;
      min-height: 0;
      width: 100%;
      height: 100%;
      resize: none;
      padding-left: var(--space-2);
      background: var(--bgColor-transparent);
      font-family: var(--fontStack-monospace);
      font-size: var(--text-codeBlock-size);
      line-height: var(--text-codeBlock-lineHeight);
      box-shadow: none;
      position: relative;
      z-index: 2;
    }

    @media (max-width: var(--breakpoint-medium)) {
      :host {
        display: block;
      }

      .gutter,
      .active-line {
        display: none;
      }

      ::slotted(textarea.sql-editor-textarea) {
        min-height: calc(var(--size-editor-min-height) + var(--overlay-height-small));
      }
    }
  `);customElements.get("sql-editor-surface")||customElements.define("sql-editor-surface",k);})();
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
*/
