package ui

const themeInitScript = `(function(){
  var root=document.documentElement;
  var media=window.matchMedia('(prefers-color-scheme: dark)');
  function normalize(mode){
    return mode==='light'||mode==='dark'||mode==='auto'?mode:'auto';
  }
  function apply(mode){
    var selected=normalize(mode);
    var resolved=selected==='auto'?(media.matches?'dark':'light'):selected;
    root.setAttribute('data-color-mode',selected);
    root.setAttribute('data-light-theme',resolved);
    root.setAttribute('data-dark-theme','dark');
  }
  var stored='auto';
  try {
    stored=normalize(localStorage.getItem('duck-ui-theme')||'auto');
  } catch (_) {}
  apply(stored);
  window.__duckUIThemeApply=apply;
})();`

const themeBehaviorScript = `(function(){
  var root=document.documentElement;
  var media=window.matchMedia('(prefers-color-scheme: dark)');
  var apply=window.__duckUIThemeApply||function(mode){
    var selected=mode==='light'||mode==='dark'||mode==='auto'?mode:'auto';
    var resolved=selected==='auto'?(media.matches?'dark':'light'):selected;
    root.setAttribute('data-color-mode',selected);
    root.setAttribute('data-light-theme',resolved);
    root.setAttribute('data-dark-theme','dark');
  };

  function selectedMode(){
    return root.getAttribute('data-color-mode')||'auto';
  }

  function resolvedMode(){
    var selected=selectedMode();
    return selected==='auto'?(media.matches?'dark':'light'):selected;
  }

  function setMode(mode){
    apply(mode);
    try { localStorage.setItem('duck-ui-theme', mode); } catch (_) {}
    syncThemeToggle();
  }

  function syncThemeToggle(){
    var toggle=document.getElementById('theme-toggle');
    if(!toggle){ return; }
    var isDark=resolvedMode()==='dark';
    var sun=document.getElementById('theme-icon-sun');
    var moon=document.getElementById('theme-icon-moon');
    if(sun){ sun.classList.toggle('is-hidden', isDark); }
    if(moon){ moon.classList.toggle('is-hidden', !isDark); }
    var nextMode=isDark?'light':'dark';
    var label=isDark?'Switch to light theme':'Switch to dark theme';
    toggle.setAttribute('aria-label', label);
    toggle.setAttribute('title', label);
    toggle.setAttribute('data-next-theme', nextMode);
  }

  var select=document.getElementById('theme-mode');
  if(select){
    select.value=selectedMode();
    select.addEventListener('change',function(e){
      var mode=e.target&&e.target.value?e.target.value:'auto';
      setMode(mode);
    });
  }

  var toggle=document.getElementById('theme-toggle');
  if(toggle){
    toggle.addEventListener('click', function(){
      var current=resolvedMode();
      setMode(current==='dark'?'light':'dark');
    });
  }

  syncThemeToggle();

  var onSystemThemeChange=function(){
    if(selectedMode()==='auto'){
      apply('auto');
      syncThemeToggle();
    }
  };
  if(media.addEventListener){
    media.addEventListener('change', onSystemThemeChange);
  } else if(media.addListener){
    media.addListener(onSystemThemeChange);
  }
})();`

const shellBehaviorScript = `(function(){
  var shell=document.querySelector('.app-shell');
  if(!shell){ return; }
  var navToggle=document.getElementById('nav-toggle');
  var sidebarToggle=document.getElementById('sidebar-toggle');
  var overlay=document.getElementById('app-overlay');
  var sidebar=document.getElementById('app-sidebar');
  var compactKey='duck-ui-sidebar-compact';

  function syncNavState(){
    var open=shell.classList.contains('nav-open');
    if(navToggle){ navToggle.setAttribute('aria-expanded', open ? 'true' : 'false'); }
    if(overlay){ overlay.setAttribute('aria-hidden', open ? 'false' : 'true'); }
  }

  function setCompact(enabled){
    shell.classList.toggle('sidebar-compact', !!enabled);
    try { localStorage.setItem(compactKey, enabled ? '1' : '0'); } catch (_) {}
  }

  try {
    setCompact(localStorage.getItem(compactKey)==='1');
  } catch (_) {}

  if(sidebarToggle){
    sidebarToggle.addEventListener('click', function(){
      setCompact(!shell.classList.contains('sidebar-compact'));
    });
  }

  if(navToggle){
    navToggle.addEventListener('click', function(){
      shell.classList.toggle('nav-open');
      syncNavState();
    });
  }

  if(overlay){
    overlay.addEventListener('click', function(){
      shell.classList.remove('nav-open');
      syncNavState();
    });
  }

  if(sidebar){
    sidebar.addEventListener('click', function(e){
      var t=e.target;
      if(!(t instanceof Element)){ return; }
      if(window.matchMedia('(max-width: 48rem)').matches && t.closest('a.app-nav-link')){
        shell.classList.remove('nav-open');
        syncNavState();
      }
    });
  }

  syncNavState();
})();`
