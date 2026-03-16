package core

import (
	"encoding/json"
	"io/fs"
	"path"
	"strings"
	"sync"

	"duck-demo/internal/ui/assets"
)

const DefaultScriptPrefix = "/ui/static/js/"

const ShellBehaviorScript = `(function(){
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

  function setCompact(enabled, persist){
    shell.classList.toggle('sidebar-compact', !!enabled);
    if(persist){
      try { localStorage.setItem(compactKey, enabled ? '1' : '0'); } catch (_) {}
    }
  }

  try {
    setCompact(localStorage.getItem(compactKey)==='1', false);
  } catch (_) {}

  try {
    var hasCompactPreference=localStorage.getItem(compactKey)!==null;
    if(!hasCompactPreference && window.matchMedia('(max-width: 48rem)').matches){
      setCompact(true, false);
    }
  } catch (_) {}

  if(sidebarToggle){
    sidebarToggle.addEventListener('click', function(){
      setCompact(!shell.classList.contains('sidebar-compact'), true);
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

var (
	scriptManifestOnce sync.Once
	scriptManifest     map[string]string
)

func UIScriptHref(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || path.Base(name) != name || path.Ext(name) != ".js" {
		return DefaultScriptPrefix + "notebook.js"
	}

	scriptManifestOnce.Do(func() {
		scriptManifest = map[string]string{}
		manifestBytes, err := fs.ReadFile(assets.StaticFS(), "static/js/manifest.json")
		if err != nil {
			return
		}

		if err := json.Unmarshal(manifestBytes, &scriptManifest); err != nil {
			scriptManifest = map[string]string{}
		}
	})

	target := name
	if hashed := strings.TrimSpace(scriptManifest[name]); hashed != "" && path.Base(hashed) == hashed && path.Ext(hashed) == ".js" {
		target = hashed
	}

	return DefaultScriptPrefix + target
}
