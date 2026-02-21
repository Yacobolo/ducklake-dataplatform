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
  var select=document.getElementById('theme-mode');
  if(select){
    select.value=root.getAttribute('data-color-mode')||'auto';
    select.addEventListener('change',function(e){
      var mode=e.target&&e.target.value?e.target.value:'auto';
      apply(mode);
      try { localStorage.setItem('duck-ui-theme', mode); } catch (_) {}
    });
  }
  var onSystemThemeChange=function(){
    if((root.getAttribute('data-color-mode')||'auto')==='auto'){ apply('auto'); }
  };
  if(media.addEventListener){
    media.addEventListener('change', onSystemThemeChange);
  } else if(media.addListener){
    media.addListener(onSystemThemeChange);
  }
})();`
