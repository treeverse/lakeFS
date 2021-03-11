
window.onload = function() {
    // Begin Swagger UI call region
    const ui = SwaggerUIBundle({
        url: "../assets/js/swagger.yml",
        dom_id: '#swagger-ui',
        deepLinking: true,
        validatorUrl: null,
        supportedSubmitMethods: [],
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [],
        layout: "BaseLayout"
    })
    // End Swagger UI call region

    window.ui = ui
}
