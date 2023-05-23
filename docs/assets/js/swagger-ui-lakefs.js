
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
    
    // Added this from https://github.com/swagger-api/swagger-ui/issues/1369
    var elemId = window.location.hash.replace('!/', '').replace('/', '').replace('#', '');
    setTimeout(function () {
        var elem = document.getElementById(elemId);
        if (elem) {
            var anchor = elem.querySelector('.model');
            if (anchor) {
                anchor.click();
            }
            setTimeout(function () {
                var top = elem.offsetTop;
                window.scrollTo({ top: top, behavior: 'auto' });
            }, 100);
        }
    }, 700);
}
