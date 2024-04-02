import "https://cdn.jsdelivr.net/gh/orestbida/cookieconsent@v3.0.0/dist/cookieconsent.umd.js";

CookieConsent.run({
  guiOptions: {
    consentModal: {
      layout: "box inline",
      position: "middle center",
      equalWeightButtons: false,
    },
    preferencesModal: {
      layout: "box",
      position: "middle center",
      equalWeightButtons: false,
      flipButtons: false,
    },
  },
  autoShow: true,
  manageScriptTags: true,
  revision: 1,
  autoClearCookies: true,
  disablePageInteraction: true,
  categories: {
    necessary: {
      enabled: true,
      readOnly: true,
    },
    analytics: {
      enabled: true,
      readOnly: false,
    },
  },
  language: {
    default: "en",
    translations: {
      en: {
        consentModal: {
          title:
            '<div style="display:flex;flex-direction:column;align-items:center;"><img style="padding-bottom: 14px;" src="data:image/png;base64,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" /><div>Hello fellow axolotl, it\'s cookie time!</div></div>',
          description:
            'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
          acceptAllBtn: "Accept all",
          showPreferencesBtn: "Settings",
          revisionMessage:
            "<br><br> Dear user, terms and conditions have changed since the last time you visited!",
        },
        preferencesModal: {
          title: "Cookie settings",
          acceptAllBtn: "Accept all",
          acceptNecessaryBtn: "Reject all",
          savePreferencesBtn: "Save current selection",
          closeIconLabel: "Close",
          sections: [
            {
              title: "Cookie usage",
              description:
                'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
            },
            {
              title: "Strictly necessary cookies",
              description:
                "These cookies are strictly necessary for the website to function. They are usually set to handle only your actions in response to a service request, such as setting your privacy preferences, navigating between pages, and setting your preferred version. You can set your browser to block these cookies or to alert you to their presence, but some parts of the website will not function without them. These cookies do not store any personally identifiable information.",
              linkedCategory: "necessary",
            },
            {
              title: "Analytics & Performance cookies",
              description:
                "These cookies are used for analytics and performance metering purposes. They are used to collect information about how visitors use our website, which helps us improve it over time. They do not collect any information that identifies a visitor. The information collected is aggregated and anonymous.",
              linkedCategory: "analytics",
            },
            {
              title: "More information",
              description:
                'For more information about cookie usage, privacy, and how we use the data we collect, please refer to our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a> and <a href="https://lakefs.io/terms-of-use/" target="_blank">terms of use</a>.',
            },
          ],
        },
      },
    },
  },
});
