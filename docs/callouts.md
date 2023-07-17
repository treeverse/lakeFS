---
layout: default
title: lakeFS Documentation - Callouts
description: Using and Customising Callouts in lakeFS Documentation
parent: Contributing
nav_order: 10
has_children: false
---

# Callouts in lakeFS Documentation

These are part of the [just-the-docs](https://just-the-docs.github.io/just-the-docs/) theme, and use the [Block Inline Attribute List (IAL) feature of Kramdown](https://kramdown.gettalong.org/syntax.html#block-ials).

## Syntax

Whilst the lakeFS docs typically use this syntax with the code after the block: 

```markdown
This is a note
{: .note }
```

This is a note
{: .note }

The just-the-docs documentation puts the code _before_ the block, which makes more sense to the human eye when reading the Markdown too. 

```
{: .note }
This is a note
```

{: .note }
This is a note

However, they render the same, as can be see above. 

## Configuration

Callouts are configured in the `_config.yml` file, with an identifier (such as `note`), a colour, and _optionally_ a title. 

```yaml
callouts:
  note:
    color: blue
  warning:
    title: ⚠️ Warning ⚠️
    color: red
  tip:
    color: green
  fubar:
    color: purple
```

Here's what the above callouts look like; notice that only `warning` includes a title: 

```yaml
{: .note }
This is a note
```

{: .note }
This is a note

```
{: .warning }
This is a warning
```

{: .warning }
This is a warning

```
{: .fubar }
This is illustrating that the callout names are completely discretionary and not tied to their meaning
```

{: .fubar }
This is illustrating that the callout names are completely discretionary and not tied to their meaning

## Custom titles

You can specify custom titles per block by appending `-title` to the block name and putting the custom title in the first line of the quoted block:

```
{: .tip-title }
> lakeFS Cloud
>
> For a fully-managed lakeFS solution check out https://lakefs.cloud/ today
```

{: .tip-title }
> lakeFS Cloud
>
> For a fully-managed lakeFS solution check out https://lakefs.cloud/ today

You can also remove the title of a callout that includes one by default (such as `warning`) above: 

```
{: .warning-title }
> Don't press the red button
```

{: .warning-title }
> Don't press the red button

Or change the default title: 

```
{: .warning-title }
> Here Be Dragons 🐲
>
> ALLES TURISTEN UND NONTEKNISCHEN LOOKENSPEEPERS! <br/>
> DAS KOMPUTERMASCHINE IST NICHT FÜR DER GEFINGERPOKEN UND MITTENGRABEN! ODERWISE IST EASY TO SCHNAPPEN DER SPRINGENWERK, BLOWENFUSEN UND POPPENCORKEN MIT SPITZENSPARKEN.
```

{: .warning-title }
> Here Be Dragons 🐲
>
> ALLES TURISTEN UND NONTEKNISCHEN LOOKENSPEEPERS! <br/>
> DAS KOMPUTERMASCHINE IST NICHT FÜR DER GEFINGERPOKEN UND MITTENGRABEN! ODERWISE IST EASY TO SCHNAPPEN DER SPRINGENWERK, BLOWENFUSEN UND POPPENCORKEN MIT SPITZENSPARKEN.

## Multi-line Blocks 

Callouts support multiple lines and/or paragraphs of content: 

```markdown
{: .note }
> A multi-paragraph block
>
> with <br/>
> linebreaks
```

{: .note }
> A multi-paragraph block
>
> with <br/>
> linebreaks


## Reference

* [Just the Docs: Callouts overview](https://just-the-docs.github.io/just-the-docs/docs/ui-components/callouts/)
* [Just the Docs: Configuration for Callouts](https://just-the-docs.github.io/just-the-docs/docs/configuration/#callouts)
* [Kramdown: Block attributes Quick Reference](https://kramdown.gettalong.org/quickref.html#block-attributes)
* [Kramdown: Syntax Guide for Block Inline Attribute List (IAL)](https://kramdown.gettalong.org/syntax.html#block-ials)