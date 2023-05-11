var LOREM_IPSUM =
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
var cc = initCookieConsent();

cc.run({
  current_lang: "en",
  page_scripts: true,
  revision: 1,
  autoclear_cookies: true,

  languages: {
    en: {
      consent_modal: {
        title:
          '<div style="display:flex;flex-direction:column;align-items:center;"><img style="padding-bottom:14px;" src="/assets/img/cookies.png" /><div>Hello fellow axolotl, it\'s cookie time!</div></div>',
        description:
          'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
        primary_btn: {
          text: "Accept all",
          role: "accept_all", //'accept_selected' or 'accept_all'
        },
        secondary_btn: {
          text: "Preferences",
          role: "settings", //'settings' or 'accept_necessary'
        },
        revision_message:
          "<br><br> Dear user, terms and conditions have changed since the last time you visited!",
      },
      settings_modal: {
        title: "Cookie settings",
        save_settings_btn: "Save current selection",
        accept_all_btn: "Accept all",
        reject_all_btn: "Reject all",
        close_btn_label: "Close",
        cookie_table_headers: [
          { col1: "Name" },
          { col2: "Domain" },
          { col3: "Expiration" },
        ],
        blocks: [
          {
            title: "Cookie usage",
            description:
              'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
          },
          {
            title: "Strictly necessary cookies",
            description:
              "These cookies are strictly necessary for the website to function. They are usually set to handle only your actions in response to a service request, such as setting your privacy preferences, navigating between pages, and setting your preferred version. You can set your browser to block these cookies or to alert you to their presence, but some parts of the website will not function without them. These cookies do not store any personally identifiable information.",
            toggle: {
              value: "necessary",
              enabled: true,
              readonly: true, //cookie categories with readonly=true are all treated as "necessary cookies"
            },
          },
          {
            title: "Analytics & Performance cookies",
            description:
              "These cookies are used for analytics and performance metering purposes. They are used to collect information about how visitors use our website, which helps us improve it over time. They do not collect any information that identifies a visitor. The information collected is aggregated and anonymous.",
            toggle: {
              value: "analytics",
              enabled: false,
              readonly: false,
            },
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
});
