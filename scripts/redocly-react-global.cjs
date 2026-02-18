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
