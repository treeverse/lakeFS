$(() => {
    let tag = `<div id='feedback-dialog'>
            <a href="#" class="x-btn"><i class="far fa-xs fa-x"></i></a>
            <div id="is-helpful-question">
                <div>Is this page helpful?</div>
                <div class="mt-1">
                    <button class="page-helpful-btn" id="page-helpful-yes" title="Yes!"><i class="far fa-lg fa-face-grin"></i></button>&#160;
                    <button class="page-helpful-btn" id="page-helpful-meh" title="Meh"><i class="far fa-lg fa-face-meh"></i></button>&#160;
                    <button class="page-helpful-btn" id="page-helpful-no" title="No, not helpful!"><i class="far fa-lg fa-face-frown-open"></i></button>
                </div>
            </div>
            <div id="is-helpful-ty">
                <div>Thank you for your feedback.</div>
                <div class="mt-2 text-epsilon"><a href="https://lakefs.io/slack">Join the community</a> to get more help.</div>
            </div>    
          </div>`
    $(tag)
        .dialog({
        resizable: false,
        autoOpen: false,
    });
    $('#feedback-dialog').parent().css({
        position: "fixed",
        justifyContent: "center",
        textAlign: "center",
        weight: "100%",
        marginBottom: "20px",
        left: "50%",
        top: "50%",
        transform: "translate(-50%, 0)",
    }).end().dialog('open');
    $("#feedback-dialog").parent().css({"max-height": "80px", "top": "unset", "bottom":0, "left": "50%", "right": "50%" }).removeClass("ui-widget");
    $(".page-helpful-btn").on("click",(ev)=> {
        $("#is-helpful-question").hide()
        $("#is-helpful-ty").show()
    })
    $("#feedback-dialog .x-btn").on("click", ()=> {
        $("#feedback-dialog").dialog("close");
        return false;
    });
})
