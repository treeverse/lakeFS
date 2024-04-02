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
            '<div style="display:flex;flex-direction:column;align-items:center;"><img style="padding-bottom:14px;" src="/assets/img/cookies.png" /><div>Hello fellow axolotl, it\'s cookie time!</div></div>',
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
