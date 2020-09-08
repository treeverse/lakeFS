$("div.highlighter-rouge").prepend("<div class=\"copy-code-container\"><button class=\"copy-code-button\" aria-label=\"Copy code block to your clipboard\"></button></div>")

$("div.highlighter-rouge .copy-code-button").click((e)=> {
    const tempTextArea = document.createElement('textarea');
    tempTextArea.textContent = $(e.target).parent().parent().find("code").text()
    document.body.appendChild(tempTextArea);
    const selection = document.getSelection();
    selection.removeAllRanges();
    tempTextArea.select();
    document.execCommand('copy');
    selection.removeAllRanges();
    document.body.removeChild(tempTextArea);

});
