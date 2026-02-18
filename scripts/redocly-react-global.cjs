if (typeof global.React === "undefined") {
  global.React = {
    createContext: function createContext() {
      return {
        Provider: function Provider() {
          return null;
        },
        Consumer: function Consumer() {
          return null;
        },
      };
    },
  };
}

if (typeof globalThis.React === "undefined") {
  globalThis.React = global.React;
}

if (typeof React === "undefined") {
  React = global.React;
}
