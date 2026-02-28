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

  var asideStoragePrefix='duck-ui-workspace-aside-tab:';
  var asideCollapsedPrefix='duck-ui-workspace-aside-collapsed:';
  var asides=document.querySelectorAll('[data-workspace-aside="true"]');
  asides.forEach(function(aside){
    if(!(aside instanceof HTMLElement)){ return; }
    var layout=aside.closest('[data-workspace-layout="true"]');
    if(!(layout instanceof HTMLElement)){ return; }
    var defaultTab=aside.getAttribute('data-workspace-aside-default')||'';
    var storageName=aside.getAttribute('data-workspace-aside-storage')||'';
    var buttons=aside.querySelectorAll('[data-workspace-aside-tab]');
    var panels=aside.querySelectorAll('[data-workspace-aside-panel]');
    var toggle=aside.querySelector('[data-workspace-aside-toggle="true"]');
    if(!buttons.length || !panels.length){ return; }

    function setCollapsed(collapsed, persist){
      layout.classList.toggle('is-aside-collapsed', collapsed);
      if(toggle instanceof HTMLElement){
        toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        toggle.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
        toggle.setAttribute('title', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
      }
      if(persist && storageName){
        try { localStorage.setItem(asideCollapsedPrefix+storageName, collapsed ? '1' : '0'); } catch (_) {}
      }
    }

    function setActive(tabID, persist){
      buttons.forEach(function(button){
        if(!(button instanceof HTMLElement)){ return; }
        var id=button.getAttribute('data-workspace-aside-tab');
        var active=id===tabID;
        button.classList.toggle('is-active', active);
        button.setAttribute('aria-selected', active ? 'true' : 'false');
      });

      panels.forEach(function(panel){
        if(!(panel instanceof HTMLElement)){ return; }
        var id=panel.getAttribute('data-workspace-aside-panel');
        panel.classList.toggle('is-active', id===tabID);
      });

      if(persist && storageName){
        try { localStorage.setItem(asideStoragePrefix+storageName, tabID); } catch (_) {}
      }
    }

    var initial=defaultTab;
    if(storageName){
      try {
        var saved=localStorage.getItem(asideStoragePrefix+storageName);
        if(saved){ initial=saved; }
      } catch (_) {}
    }

    if(!initial){
      var first=buttons[0];
      if(first instanceof HTMLElement){
        initial=first.getAttribute('data-workspace-aside-tab')||'';
      }
    }

    if(initial){
      setActive(initial, false);
    }

    if(storageName){
      try {
        var collapsedState=localStorage.getItem(asideCollapsedPrefix+storageName);
        if(collapsedState==='1'){
          setCollapsed(true, false);
        }
      } catch (_) {}
    }

    aside.addEventListener('click', function(e){
      var t=e.target;
      if(!(t instanceof Element)){ return; }
      var toggleButton=t.closest('[data-workspace-aside-toggle="true"]');
      if(toggleButton instanceof HTMLElement){
        setCollapsed(!layout.classList.contains('is-aside-collapsed'), true);
        return;
      }
      var tab=t.closest('[data-workspace-aside-tab]');
      if(!(tab instanceof HTMLElement)){ return; }
      var tabID=tab.getAttribute('data-workspace-aside-tab')||'';
      if(!tabID){ return; }
      if(layout.classList.contains('is-aside-collapsed')){
        setCollapsed(false, true);
      }
      setActive(tabID, true);
    });
  });

  syncNavState();
})();`
