window.onload = function () {
  // Begin Swagger UI call region
  const ui = SwaggerUIBundle({
    url: "swagger.yml",
    dom_id: "#swagger-ui",
    deepLinking: true,
    validatorUrl: null,
    supportedSubmitMethods: [],
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    plugins: [],
    layout: "BaseLayout",
    onComplete: () => {
      const operationId = window.location.hash
        .replace(/\//g, "-")
        .replace("#", "");
      const elem =
        operationId && document.getElementById("operations" + operationId);
      if (elem) {
        setTimeout(function () {
          elem.scrollIntoView();
        }, 100);
      }
    },
  });
  // End Swagger UI call region

  window.ui = ui;
};
