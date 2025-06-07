$(() => {
    const close = () => {
        $('#is-helpful-ty').dialog('close');
        $(document).off("click", close);
        $(window).off("scroll", close);
    }
    const showThankYou = (elm) => {
        $('#is-helpful-ty').dialog({
            width: 200,
            position: {of: elm, my: 'top right', at: 'left-50 bottom+50', collision: "fit"},
            open: () => setTimeout(() => {
                $(document).on("click", close);
                $(window).on("scroll", close);
            }, 1),
            show: {effect: "drop"},
            hide: {effect: "drop"}
        })
        setTimeout(close, 10000);
    }

    const sendFeedbackEvent = (elm) => {
        if (window.dataLayer === undefined) return; // GA4 is not loaded on this page
        const isHelpful = elm.id.substr("page-helpful-".length) === "yes";
        window.dataLayer.push("event", "click_docs_feedback", {
            "page_title": window.document.title,
            "reaction": isHelpful ? "thumbs_up" : "thumbs_down",
        });
    }

    $(".page-helpful-btn").on("click", (e) => {
        const elm = e.target;
        sendFeedbackEvent(elm);
        showThankYou(elm);
    });
    $('#is-helpful-ty').on("click", (e) => $(e.target).prop("tagName") === 'A')
})
