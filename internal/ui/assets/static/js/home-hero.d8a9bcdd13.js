"use strict";(()=>{var te=i=>{throw TypeError(i)};var _e=(i,e,t)=>e.has(i)||te("Cannot "+t);var b=(i,e,t)=>(_e(i,e,"read from private field"),t?t.call(i):e.get(i)),D=(i,e,t)=>e.has(i)?te("Cannot add the same private member more than once"):e instanceof WeakSet?e.add(i):e.set(i,t);var z=globalThis,B=z.ShadowRoot&&(z.ShadyCSS===void 0||z.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,I=Symbol(),re=new WeakMap,S=class{constructor(e,t,r){if(this._$cssResult$=!0,r!==I)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=t}get styleSheet(){let e=this.o,t=this.t;if(B&&e===void 0){let r=t!==void 0&&t.length===1;r&&(e=re.get(t)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),r&&re.set(t,e))}return e}toString(){return this.cssText}},se=i=>new S(typeof i=="string"?i:i+"",void 0,I),V=(i,...e)=>{let t=i.length===1?i[0]:e.reduce((r,s,o)=>r+(a=>{if(a._$cssResult$===!0)return a.cssText;if(typeof a=="number")return a;throw Error("Value passed to 'css' function must be a 'css' function result: "+a+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+i[o+1],i[0]);return new S(t,i,I)},ie=(i,e)=>{if(B)i.adoptedStyleSheets=e.map(t=>t instanceof CSSStyleSheet?t:t.styleSheet);else for(let t of e){let r=document.createElement("style"),s=z.litNonce;s!==void 0&&r.setAttribute("nonce",s),r.textContent=t.cssText,i.appendChild(r)}},W=B?i=>i:i=>i instanceof CSSStyleSheet?(e=>{let t="";for(let r of e.cssRules)t+=r.cssText;return se(t)})(i):i;var{is:we,defineProperty:xe,getOwnPropertyDescriptor:Ae,getOwnPropertyNames:ke,getOwnPropertySymbols:Ee,getPrototypeOf:Se}=Object,g=globalThis,oe=g.trustedTypes,Ce=oe?oe.emptyScript:"",qe=g.reactiveElementPolyfillSupport,C=(i,e)=>i,X={toAttribute(i,e){switch(e){case Boolean:i=i?Ce:null;break;case Object:case Array:i=i==null?i:JSON.stringify(i)}return i},fromAttribute(i,e){let t=i;switch(e){case Boolean:t=i!==null;break;case Number:t=i===null?null:Number(i);break;case Object:case Array:try{t=JSON.parse(i)}catch{t=null}}return t}},ne=(i,e)=>!we(i,e),ae={attribute:!0,type:String,converter:X,reflect:!1,useDefault:!1,hasChanged:ne};Symbol.metadata??(Symbol.metadata=Symbol("metadata")),g.litPropertyMetadata??(g.litPropertyMetadata=new WeakMap);var m=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??(this.l=[])).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,t=ae){if(t.state&&(t.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((t=Object.create(t)).wrapped=!0),this.elementProperties.set(e,t),!t.noAccessor){let r=Symbol(),s=this.getPropertyDescriptor(e,r,t);s!==void 0&&xe(this.prototype,e,s)}}static getPropertyDescriptor(e,t,r){let{get:s,set:o}=Ae(this.prototype,e)??{get(){return this[t]},set(a){this[t]=a}};return{get:s,set(a){let l=s?.call(this);o?.call(this,a),this.requestUpdate(e,l,r)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??ae}static _$Ei(){if(this.hasOwnProperty(C("elementProperties")))return;let e=Se(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(C("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(C("properties"))){let t=this.properties,r=[...ke(t),...Ee(t)];for(let s of r)this.createProperty(s,t[s])}let e=this[Symbol.metadata];if(e!==null){let t=litPropertyMetadata.get(e);if(t!==void 0)for(let[r,s]of t)this.elementProperties.set(r,s)}this._$Eh=new Map;for(let[t,r]of this.elementProperties){let s=this._$Eu(t,r);s!==void 0&&this._$Eh.set(s,t)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){let t=[];if(Array.isArray(e)){let r=new Set(e.flat(1/0).reverse());for(let s of r)t.unshift(W(s))}else e!==void 0&&t.push(W(e));return t}static _$Eu(e,t){let r=t.attribute;return r===!1?void 0:typeof r=="string"?r:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??(this._$EO=new Set)).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){let e=new Map,t=this.constructor.elementProperties;for(let r of t.keys())this.hasOwnProperty(r)&&(e.set(r,this[r]),delete this[r]);e.size>0&&(this._$Ep=e)}createRenderRoot(){let e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return ie(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??(this.renderRoot=this.createRenderRoot()),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,t,r){this._$AK(e,r)}_$ET(e,t){let r=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,r);if(s!==void 0&&r.reflect===!0){let o=(r.converter?.toAttribute!==void 0?r.converter:X).toAttribute(t,r.type);this._$Em=e,o==null?this.removeAttribute(s):this.setAttribute(s,o),this._$Em=null}}_$AK(e,t){let r=this.constructor,s=r._$Eh.get(e);if(s!==void 0&&this._$Em!==s){let o=r.getPropertyOptions(s),a=typeof o.converter=="function"?{fromAttribute:o.converter}:o.converter?.fromAttribute!==void 0?o.converter:X;this._$Em=s;let l=a.fromAttribute(t,o.type);this[s]=l??this._$Ej?.get(s)??l,this._$Em=null}}requestUpdate(e,t,r,s=!1,o){if(e!==void 0){let a=this.constructor;if(s===!1&&(o=this[e]),r??(r=a.getPropertyOptions(e)),!((r.hasChanged??ne)(o,t)||r.useDefault&&r.reflect&&o===this._$Ej?.get(e)&&!this.hasAttribute(a._$Eu(e,r))))return;this.C(e,t,r)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,t,{useDefault:r,reflect:s,wrapped:o},a){r&&!(this._$Ej??(this._$Ej=new Map)).has(e)&&(this._$Ej.set(e,a??t??this[e]),o!==!0||a!==void 0)||(this._$AL.has(e)||(this.hasUpdated||r||(t=void 0),this._$AL.set(e,t)),s===!0&&this._$Em!==e&&(this._$Eq??(this._$Eq=new Set)).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(t){Promise.reject(t)}let e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??(this.renderRoot=this.createRenderRoot()),this._$Ep){for(let[s,o]of this._$Ep)this[s]=o;this._$Ep=void 0}let r=this.constructor.elementProperties;if(r.size>0)for(let[s,o]of r){let{wrapped:a}=o,l=this[s];a!==!0||this._$AL.has(s)||l===void 0||this.C(s,void 0,o,l)}}let e=!1,t=this._$AL;try{e=this.shouldUpdate(t),e?(this.willUpdate(t),this._$EO?.forEach(r=>r.hostUpdate?.()),this.update(t)):this._$EM()}catch(r){throw e=!1,this._$EM(),r}e&&this._$AE(t)}willUpdate(e){}_$AE(e){this._$EO?.forEach(t=>t.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&(this._$Eq=this._$Eq.forEach(t=>this._$ET(t,this[t]))),this._$EM()}updated(e){}firstUpdated(e){}};m.elementStyles=[],m.shadowRootOptions={mode:"open"},m[C("elementProperties")]=new Map,m[C("finalized")]=new Map,qe?.({ReactiveElement:m}),(g.reactiveElementVersions??(g.reactiveElementVersions=[])).push("2.1.2");var P=globalThis,he=i=>i,j=P.trustedTypes,le=j?j.createPolicy("lit-html",{createHTML:i=>i}):void 0,fe="$lit$",v=`lit$${Math.random().toFixed(9).slice(2)}$`,ge="?"+v,Pe=`<${ge}>`,w=document,M=()=>w.createComment(""),U=i=>i===null||typeof i!="object"&&typeof i!="function",Q=Array.isArray,Me=i=>Q(i)||typeof i?.[Symbol.iterator]=="function",K=`[ 	
\f\r]`,q=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,ce=/-->/g,de=/>/g,y=RegExp(`>|${K}(?:([^\\s"'>=/]+)(${K}*=${K}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),pe=/'/g,ue=/"/g,ve=/^(?:script|style|textarea|title)$/i,ee=i=>(e,...t)=>({_$litType$:i,strings:e,values:t}),$e=ee(1),Be=ee(2),je=ee(3),x=Symbol.for("lit-noChange"),p=Symbol.for("lit-nothing"),me=new WeakMap,_=w.createTreeWalker(w,129);function be(i,e){if(!Q(i)||!i.hasOwnProperty("raw"))throw Error("invalid template strings array");return le!==void 0?le.createHTML(e):e}var Ue=(i,e)=>{let t=i.length-1,r=[],s,o=e===2?"<svg>":e===3?"<math>":"",a=q;for(let l=0;l<t;l++){let n=i[l],c,d,h=-1,u=0;for(;u<n.length&&(a.lastIndex=u,d=a.exec(n),d!==null);)u=a.lastIndex,a===q?d[1]==="!--"?a=ce:d[1]!==void 0?a=de:d[2]!==void 0?(ve.test(d[2])&&(s=RegExp("</"+d[2],"g")),a=y):d[3]!==void 0&&(a=y):a===y?d[0]===">"?(a=s??q,h=-1):d[1]===void 0?h=-2:(h=a.lastIndex-d[2].length,c=d[1],a=d[3]===void 0?y:d[3]==='"'?ue:pe):a===ue||a===pe?a=y:a===ce||a===de?a=q:(a=y,s=void 0);let f=a===y&&i[l+1].startsWith("/>")?" ":"";o+=a===q?n+Pe:h>=0?(r.push(c),n.slice(0,h)+fe+n.slice(h)+v+f):n+v+(h===-2?l:f)}return[be(i,o+(i[t]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),r]},N=class i{constructor({strings:e,_$litType$:t},r){let s;this.parts=[];let o=0,a=0,l=e.length-1,n=this.parts,[c,d]=Ue(e,t);if(this.el=i.createElement(c,r),_.currentNode=this.el.content,t===2||t===3){let h=this.el.content.firstChild;h.replaceWith(...h.childNodes)}for(;(s=_.nextNode())!==null&&n.length<l;){if(s.nodeType===1){if(s.hasAttributes())for(let h of s.getAttributeNames())if(h.endsWith(fe)){let u=d[a++],f=s.getAttribute(h).split(v),L=/([.?@])?(.*)/.exec(u);n.push({type:1,index:o,name:L[2],strings:f,ctor:L[1]==="."?Z:L[1]==="?"?F:L[1]==="@"?J:E}),s.removeAttribute(h)}else h.startsWith(v)&&(n.push({type:6,index:o}),s.removeAttribute(h));if(ve.test(s.tagName)){let h=s.textContent.split(v),u=h.length-1;if(u>0){s.textContent=j?j.emptyScript:"";for(let f=0;f<u;f++)s.append(h[f],M()),_.nextNode(),n.push({type:2,index:++o});s.append(h[u],M())}}}else if(s.nodeType===8)if(s.data===ge)n.push({type:2,index:o});else{let h=-1;for(;(h=s.data.indexOf(v,h+1))!==-1;)n.push({type:7,index:o}),h+=v.length-1}o++}}static createElement(e,t){let r=w.createElement("template");return r.innerHTML=e,r}};function k(i,e,t=i,r){if(e===x)return e;let s=r!==void 0?t._$Co?.[r]:t._$Cl,o=U(e)?void 0:e._$litDirective$;return s?.constructor!==o&&(s?._$AO?.(!1),o===void 0?s=void 0:(s=new o(i),s._$AT(i,t,r)),r!==void 0?(t._$Co??(t._$Co=[]))[r]=s:t._$Cl=s),s!==void 0&&(e=k(i,s._$AS(i,e.values),s,r)),e}var Y=class{constructor(e,t){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=t}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){let{el:{content:t},parts:r}=this._$AD,s=(e?.creationScope??w).importNode(t,!0);_.currentNode=s;let o=_.nextNode(),a=0,l=0,n=r[0];for(;n!==void 0;){if(a===n.index){let c;n.type===2?c=new O(o,o.nextSibling,this,e):n.type===1?c=new n.ctor(o,n.name,n.strings,this,e):n.type===6&&(c=new G(o,this,e)),this._$AV.push(c),n=r[++l]}a!==n?.index&&(o=_.nextNode(),a++)}return _.currentNode=w,s}p(e){let t=0;for(let r of this._$AV)r!==void 0&&(r.strings!==void 0?(r._$AI(e,r,t),t+=r.strings.length-2):r._$AI(e[t])),t++}},O=class i{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,t,r,s){this.type=2,this._$AH=p,this._$AN=void 0,this._$AA=e,this._$AB=t,this._$AM=r,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode,t=this._$AM;return t!==void 0&&e?.nodeType===11&&(e=t.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,t=this){e=k(this,e,t),U(e)?e===p||e==null||e===""?(this._$AH!==p&&this._$AR(),this._$AH=p):e!==this._$AH&&e!==x&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Me(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==p&&U(this._$AH)?this._$AA.nextSibling.data=e:this.T(w.createTextNode(e)),this._$AH=e}$(e){let{values:t,_$litType$:r}=e,s=typeof r=="number"?this._$AC(e):(r.el===void 0&&(r.el=N.createElement(be(r.h,r.h[0]),this.options)),r);if(this._$AH?._$AD===s)this._$AH.p(t);else{let o=new Y(s,this),a=o.u(this.options);o.p(t),this.T(a),this._$AH=o}}_$AC(e){let t=me.get(e.strings);return t===void 0&&me.set(e.strings,t=new N(e)),t}k(e){Q(this._$AH)||(this._$AH=[],this._$AR());let t=this._$AH,r,s=0;for(let o of e)s===t.length?t.push(r=new i(this.O(M()),this.O(M()),this,this.options)):r=t[s],r._$AI(o),s++;s<t.length&&(this._$AR(r&&r._$AB.nextSibling,s),t.length=s)}_$AR(e=this._$AA.nextSibling,t){for(this._$AP?.(!1,!0,t);e!==this._$AB;){let r=he(e).nextSibling;he(e).remove(),e=r}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}},E=class{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,t,r,s,o){this.type=1,this._$AH=p,this._$AN=void 0,this.element=e,this.name=t,this._$AM=s,this.options=o,r.length>2||r[0]!==""||r[1]!==""?(this._$AH=Array(r.length-1).fill(new String),this.strings=r):this._$AH=p}_$AI(e,t=this,r,s){let o=this.strings,a=!1;if(o===void 0)e=k(this,e,t,0),a=!U(e)||e!==this._$AH&&e!==x,a&&(this._$AH=e);else{let l=e,n,c;for(e=o[0],n=0;n<o.length-1;n++)c=k(this,l[r+n],t,n),c===x&&(c=this._$AH[n]),a||(a=!U(c)||c!==this._$AH[n]),c===p?e=p:e!==p&&(e+=(c??"")+o[n+1]),this._$AH[n]=c}a&&!s&&this.j(e)}j(e){e===p?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}},Z=class extends E{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===p?void 0:e}},F=class extends E{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==p)}},J=class extends E{constructor(e,t,r,s,o){super(e,t,r,s,o),this.type=5}_$AI(e,t=this){if((e=k(this,e,t,0)??p)===x)return;let r=this._$AH,s=e===p&&r!==p||e.capture!==r.capture||e.once!==r.once||e.passive!==r.passive,o=e!==p&&(r===p||s);s&&this.element.removeEventListener(this.name,this,r),o&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}},G=class{constructor(e,t,r){this.element=e,this.type=6,this._$AN=void 0,this._$AM=t,this.options=r}get _$AU(){return this._$AM._$AU}_$AI(e){k(this,e)}};var Ne=P.litHtmlPolyfillSupport;Ne?.(N,O),(P.litHtmlVersions??(P.litHtmlVersions=[])).push("3.3.2");var ye=(i,e,t)=>{let r=t?.renderBefore??e,s=r._$litPart$;if(s===void 0){let o=t?.renderBefore??null;r._$litPart$=s=new O(e.insertBefore(M(),o),o,void 0,t??{})}return s._$AI(i),s};var R=globalThis,$=class extends m{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){var t;let e=super.createRenderRoot();return(t=this.renderOptions).renderBefore??(t.renderBefore=e.firstChild),e}update(e){let t=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=ye(t,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return x}};$._$litElement$=!0,$.finalized=!0,R.litElementHydrateSupport?.({LitElement:$});var Oe=R.litElementPolyfillSupport;Oe?.({LitElement:$});(R.litElementVersions??(R.litElementVersions=[])).push("4.2.2");var H,A,T=class extends ${constructor(){super(...arguments);this.displayName="";D(this,H,t=>{let r=this.renderRoot.querySelector(".hero");if(!r)return;let s=r.getBoundingClientRect();if(!s.width||!s.height)return;let o=(t.clientX-s.left)/s.width-.5,a=(t.clientY-s.top)/s.height-.5,l=Math.max(-3,Math.min(3,o*7)),n=Math.max(-2,Math.min(2,a*5)),c=o*12,d=a*7,h=o*4;r.style.setProperty("--pupil-x",`${l}px`),r.style.setProperty("--pupil-y",`${n}px`),r.style.setProperty("--quack-shift-x",`${c}px`),r.style.setProperty("--quack-shift-y",`${d}px`),r.style.setProperty("--quack-tilt",`${h}deg`)});D(this,A,()=>{let t=this.renderRoot.querySelector(".hero");t&&(t.style.setProperty("--pupil-x","0px"),t.style.setProperty("--pupil-y","0px"),t.style.setProperty("--quack-shift-x","0px"),t.style.setProperty("--quack-shift-y","0px"),t.style.setProperty("--quack-tilt","0deg"))})}connectedCallback(){super.connectedCallback(),window.addEventListener("pointermove",b(this,H)),window.addEventListener("pointerleave",b(this,A)),window.addEventListener("blur",b(this,A))}disconnectedCallback(){window.removeEventListener("pointermove",b(this,H)),window.removeEventListener("pointerleave",b(this,A)),window.removeEventListener("blur",b(this,A)),super.disconnectedCallback()}render(){let t=this.displayName?.trim()||"there";return $e`
      <section class="hero" aria-label="Welcome home">
        <div class="copy">
          <p class="eyebrow">Welcome back</p>
          <h2>${t}</h2>
        </div>

        <div class="scene" aria-hidden="true">
          <div class="quack">
            <div class="quack-shadow"></div>
            <div class="quack-waterline"></div>
            <div class="quack-body">
              <div class="quack-tail"></div>
              <div class="quack-wing"></div>
              <div class="quack-neck"></div>
              <div class="quack-head">
                <div class="quack-cheek"></div>
                <div class="quack-eye left"><span class="quack-pupil"></span></div>
                <div class="quack-eye right"><span class="quack-pupil"></span></div>
              </div>
            </div>
          </div>

          <svg class="waves" viewBox="0 0 1200 220" preserveAspectRatio="none">
            <path
              class="wave wave-1"
              d="M0 118C96 86 187 78 284 93C381 108 474 145 565 146C665 147 753 104 850 97C966 88 1065 121 1200 162V220H0Z"
            ></path>
            <path
              class="wave wave-2"
              d="M0 142C115 168 214 171 306 150C410 126 489 84 583 88C676 92 741 133 841 145C960 160 1066 133 1200 103V220H0Z"
            ></path>
            <path
              class="wave wave-3"
              d="M0 176C111 150 213 135 317 142C431 149 519 196 631 198C742 200 819 156 923 146C1028 136 1115 149 1200 182V220H0Z"
            ></path>
          </svg>
        </div>
      </section>
    `}};H=new WeakMap,A=new WeakMap,T.properties={displayName:{attribute:"display-name",type:String}},T.styles=V`
    :host {
      display: block;
      color: var(--fgColor-default);
      background: transparent;
      --hero-text: var(--fgColor-default);
      --hero-muted: var(--fgColor-muted);
      --hero-accent: var(--fgColor-accent);
      --hero-surface: var(--bgColor-default);
      --hero-surface-muted: var(--bgColor-muted);
      --hero-border: var(--borderColor-default);
      --hero-border-strong: var(--borderColor-accent-emphasis);
      --hero-attention: var(--bgColor-attention-emphasis);
      --hero-danger-soft: var(--bgColor-danger-muted);
      --hero-meta-bg: color-mix(in srgb, var(--hero-surface) 76%, transparent);
      --hero-meta-dot: color-mix(in srgb, var(--hero-accent) 58%, var(--hero-surface-muted) 42%);
      --hero-wave-1: color-mix(in srgb, var(--hero-accent) 10%, var(--hero-surface) 90%);
      --hero-wave-2: color-mix(in srgb, var(--hero-accent) 18%, var(--hero-surface-muted) 82%);
      --hero-wave-3: color-mix(in srgb, var(--hero-accent) 28%, var(--hero-surface-muted) 72%);
      --hero-quack-shadow: color-mix(in srgb, var(--hero-text) 12%, transparent);
      --hero-light: color-mix(in srgb, var(--hero-surface) 72%, var(--hero-text) 28%);
      --hero-light-strong: color-mix(in srgb, var(--hero-surface) 88%, var(--hero-text) 12%);
      --hero-light-soft: color-mix(in srgb, var(--hero-surface) 56%, transparent);
      --hero-quack-feather-top: color-mix(in srgb, var(--display-yellow-bgColor-muted) 74%, var(--hero-light-strong) 26%);
      --hero-quack-feather-mid: color-mix(in srgb, var(--display-yellow-bgColor-muted) 44%, var(--display-yellow-fgColor) 56%);
      --hero-quack-feather-low: color-mix(in srgb, var(--display-yellow-fgColor) 82%, var(--display-yellow-bgColor-muted) 18%);
      --hero-quack-highlight: color-mix(in srgb, var(--hero-light) 75%, transparent);
      --hero-quack-shade: color-mix(in srgb, var(--hero-border) 44%, transparent);
      --hero-beak-top: color-mix(in srgb, var(--hero-attention) 78%, var(--hero-light-strong) 22%);
      --hero-beak-bottom: color-mix(in srgb, var(--hero-attention) 88%, var(--hero-text) 12%);
      --hero-eye-white: color-mix(in srgb, white 92%, var(--hero-surface) 8%);
      --hero-eye-border: color-mix(in srgb, var(--hero-border) 72%, transparent);
      --hero-pupil: color-mix(in srgb, black 88%, var(--hero-surface) 12%);
      --hero-cheek: color-mix(in srgb, var(--hero-danger-soft) 48%, transparent);
      --hero-waterline: color-mix(in srgb, var(--hero-light-soft) 42%, var(--hero-surface) 58%);
    }

    * {
      box-sizing: border-box;
    }

    .hero {
      position: relative;
      min-height: 20rem;
      padding: clamp(0.5rem, 1vw, 0.9rem) 0 0;
      overflow: hidden;
    }

    .copy {
      position: relative;
      z-index: 3;
      display: grid;
      gap: 0.55rem;
      max-width: 36rem;
      padding-top: 0.35rem;
      padding-left: clamp(0rem, 1vw, 0.5rem);
    }

    .eyebrow {
      margin: 0;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      color: var(--hero-accent);
    }

    h2 {
      margin: 0;
      font-size: clamp(2rem, 4.2vw, 3.9rem);
      line-height: 0.94;
      letter-spacing: -0.065em;
      font-weight: 700;
      text-wrap: balance;
    }

    p {
      margin: 0;
    }

    .scene {
      position: absolute;
      inset: 0 -3% 0 -3%;
      overflow: hidden;
      mask-image: linear-gradient(180deg, transparent 0%, black 12%, black 100%);
    }

    .quack {
      position: absolute;
      right: 10%;
      bottom: 3.1rem;
      width: min(18rem, 92%);
      aspect-ratio: 1.1;
      transform:
        translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 2px), 0)
        rotate(calc(var(--quack-tilt, 0deg) + 0deg));
      transition: transform 220ms cubic-bezier(0.22, 1, 0.36, 1);
      animation: quack-bob 4.6s ease-in-out infinite;
      transform-origin: center 72%;
      will-change: transform;
    }

    .quack-shadow {
      position: absolute;
      left: 29%;
      bottom: 1rem;
      width: 42%;
      height: 1rem;
      border-radius: 50%;
      background: var(--hero-quack-shadow);
      filter: blur(10px);
      animation: shadow-sway 4.6s ease-in-out infinite;
    }

    .quack-body {
      position: absolute;
      left: 20%;
      bottom: 1.2rem;
      width: 58%;
      height: 38%;
      border-radius: 54% 46% 48% 52% / 58% 56% 44% 42%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      box-shadow:
        inset 0 -0.8rem 0 var(--hero-quack-shade),
        inset 0 0.2rem 0 var(--hero-quack-highlight);
    }

    .quack-tail {
      position: absolute;
      left: -2%;
      top: 34%;
      width: 22%;
      height: 22%;
      border-radius: 30% 70% 30% 70%;
      background: var(--hero-quack-feather-top);
      transform: rotate(-24deg);
    }

    .quack-wing {
      position: absolute;
      left: 31%;
      top: 18%;
      width: 34%;
      height: 42%;
      border-radius: 50% 50% 48% 52% / 56% 54% 46% 44%;
      background: linear-gradient(180deg, var(--hero-quack-feather-mid) 0%, var(--hero-quack-feather-low) 100%);
      transform: rotate(-12deg);
      box-shadow: inset 0 0.12rem 0 color-mix(in srgb, var(--hero-light-soft) 36%, transparent);
    }

    .quack-neck {
      position: absolute;
      right: 22%;
      bottom: 44%;
      width: 20%;
      height: 28%;
      border-radius: 48% 52% 46% 54% / 20% 20% 80% 80%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      transform: rotate(-10deg);
      transform-origin: bottom center;
    }

    .quack-head {
      position: absolute;
      right: 7%;
      bottom: 59%;
      width: 30%;
      height: 26%;
      border-radius: 52% 48% 50% 50% / 48% 48% 52% 52%;
      background: linear-gradient(180deg, var(--hero-quack-feather-top) 0%, var(--hero-quack-feather-mid) 100%);
      box-shadow: inset 0 0.14rem 0 color-mix(in srgb, var(--hero-light) 58%, transparent);
    }

    .quack-head::after {
      content: "";
      position: absolute;
      left: 70%;
      bottom: 16%;
      width: 44%;
      height: 28%;
      border-radius: 48% 52% 54% 46% / 50% 50% 50% 50%;
      background: linear-gradient(180deg, var(--hero-beak-top) 0%, var(--hero-beak-bottom) 100%);
      transform: rotate(4deg);
      box-shadow: inset 0 -0.1rem 0 color-mix(in srgb, var(--hero-text) 18%, transparent);
    }

    .quack-eye {
      position: absolute;
      top: 34%;
      width: 0.95rem;
      height: 0.95rem;
      border-radius: 50%;
      background: var(--hero-eye-white);
      box-shadow: inset 0 0 0 1px var(--hero-eye-border);
      overflow: hidden;
    }

    .quack-eye.left {
      left: 34%;
    }

    .quack-eye.right {
      left: 54%;
    }

    .quack-pupil {
      position: absolute;
      left: 50%;
      top: 50%;
      width: 0.38rem;
      height: 0.38rem;
      border-radius: 50%;
      background: var(--hero-pupil);
      transform: translate(calc(-50% + var(--pupil-x, 0px)), calc(-50% + var(--pupil-y, 0px)));
      transition: transform 70ms linear;
    }

    .quack-cheek {
      position: absolute;
      left: 22%;
      bottom: 18%;
      width: 16%;
      height: 12%;
      border-radius: 50%;
      background: var(--hero-cheek);
      filter: blur(4px);
    }

    .quack-waterline {
      position: absolute;
      left: 22%;
      bottom: 3.3rem;
      width: 54%;
      height: 0.28rem;
      border-radius: 999px;
      background: var(--hero-waterline);
      opacity: 0.8;
      filter: blur(1px);
    }

    .waves {
      position: absolute;
      left: -2%;
      right: -2%;
      bottom: 0;
      width: 104%;
      height: 10.75rem;
      pointer-events: none;
    }

    .wave {
      animation: wave-drift 13s cubic-bezier(0.55, 0.5, 0.45, 0.5) infinite alternate;
      transform-origin: center;
    }

    .wave-2 {
      fill: var(--hero-wave-2);
      animation-duration: 17s;
      animation-delay: -3s;
    }

    .wave-3 {
      fill: var(--hero-wave-3);
      animation-duration: 22s;
      animation-delay: -5s;
    }

    .wave-1 {
      fill: var(--hero-wave-1);
    }

    @keyframes quack-bob {
      0%,
      100% {
        transform:
          translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 2px), 0)
          rotate(calc(var(--quack-tilt, 0deg) - 0.6deg));
      }
      50% {
        transform:
          translate3d(var(--quack-shift-x, 0px), calc(var(--quack-shift-y, 0px) - 12px), 0)
          rotate(calc(var(--quack-tilt, 0deg) + 0.7deg));
      }
    }

    @keyframes shadow-sway {
      0%,
      100% {
        transform: scaleX(0.96);
        opacity: 0.22;
      }
      50% {
        transform: scaleX(1.06);
        opacity: 0.13;
      }
    }

    @keyframes wave-drift {
      0% {
        transform: translate3d(-1.8%, 0.3rem, 0);
      }
      100% {
        transform: translate3d(1.8%, -0.3rem, 0);
      }
    }

    @media (max-width: 56rem) {
      .hero {
        min-height: 17.5rem;
      }

      .copy {
        max-width: 28rem;
        padding-top: 0.25rem;
      }

      .quack {
        right: 50%;
        width: min(15rem, 76%);
        transform: translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 2px), 0) rotate(var(--quack-tilt, 0deg));
      }

      @keyframes quack-bob {
        0%,
        100% {
          transform:
            translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 2px), 0)
            rotate(calc(var(--quack-tilt, 0deg) - 0.6deg));
        }
        50% {
          transform:
            translate3d(calc(50% + var(--quack-shift-x, 0px)), calc(var(--quack-shift-y, 0px) - 12px), 0)
            rotate(calc(var(--quack-tilt, 0deg) + 0.7deg));
        }
      }
    }

    @media (max-width: 42rem) {
      .hero {
        min-height: 16rem;
      }

      .scene {
        inset: 0 -8% 0 -8%;
      }
    }

    @media (prefers-reduced-motion: reduce) {
      .quack,
      .quack-shadow,
      .wave {
        animation: none;
        transition: none;
      }
    }
  `;customElements.get("quack-home-hero")||customElements.define("quack-home-hero",T);})();
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
