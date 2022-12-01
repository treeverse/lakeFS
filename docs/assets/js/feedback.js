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
    $(".page-helpful-btn").on("click", (e) => showThankYou(e.target))
    $('#is-helpful-ty').on("click", (e) => $(e.target).prop("tagName") === 'A')
})
