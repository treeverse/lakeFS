# lakeFS Documentation

Any contribution to the docs, whether it is in conjunction with a code contribution or as a standalone, is appreciated.

Please see [the contributing guide](contributing.md) for details on contributing to lakeFS in general. 

📝  Notice! lakeFS documentation is written using Markdown. make sure to familiarize yourself with the [Markdown Guide](https://www.markdownguide.org/basic-syntax/#heading-best-practices).

Customizing the lakeFS docs site should follow the following guidelines: [Just The Docs Customization](https://just-the-docs.github.io/just-the-docs/docs/customization/) and style-guide.

### lakeFS Style Guide:
* Don't use unnecessary tech jargon or vague/wordy constructions - keep it friendly, not condescending.
* Be inclusive and welcoming - use gender-neutral words and pronouns when talking about abstract people like developers).
* Replace complex expressions with simpler ones.
* Keep it short - 25-30 words max per sentence.  Otherwise, your readers might get lost on the way. 
* Use active voice instead of passive. For example: This feature can be used to do task X. vs. You can use this feature to do task X. The second one reads much better, right?
* You can explain things better by including examples. Show, not tell. Use illustrations, images, gifs, code snippets, etc.
* Establish a visual hierarchy to help people quickly find the information they need. Use text formatting to create levels of title and subtitle (such as h1 to h6 headings in HTML).

### Test your changes locally

To render the documentation locally and preview changes you can run the Jeykll server under Docker. 

1. Launch the Docker container:

   ```sh
   docker run --rm \
              --name lakefs_docs \
              -e TZ="Etc/UTC" \
              --publish 4000:4000 --publish 35729:35729 \
              --volume="$PWD/docs:/srv/jekyll:Z" \
              --volume="$PWD/docs/.jekyll-bundle-cache:/usr/local/bundle:Z" \
              --interactive --tty \
              jekyll/jekyll:3.8 \
              jekyll serve --livereload
   ```

2. The first time you run the container it will need to download dependencies and will take several minutes to be ready. 

   Once you see the following output, the docs server is ready to [open in your web browser](http://localhost:4000): 

   ```
   Server running... press ctrl-c to stop.
   ```

3. When you make a change to a page's source the server will automatically rebuild the page which will be shown in the server log by this entry:

   ```
   Regenerating: 1 file(s) changed at 2023-01-26 08:34:47
                  contributing.md
   Remote Theme: Using theme pmarsceill/just-the-docs
   ```

   This can take a short while—you'll see something like this in the server's output when it's done. 
   
   ```
   ...done in 34.714073460 seconds.
   ```

   Your page will automatically reload to show the changes.

_If you are doing lots of work on the docs you may want to leave the Docker container in place (so that you don't have to wait for the dependencies to load each time you re-create it). To do this replace the `--rm` with `--detach` in the `docker run` command, and use `docker logs -f lakefs_docs` to view the server log._

### Link Checking locally

When making a pull request to lakeFS that involves a `docs/*` file, a [GitHub action](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/docs-pr.yaml) will automagically check the links. You can also run this link checker manually on your local machine: 

1. Build the site: 

   ```
   docker run --rm \
            --name lakefs_docs \
            -e TZ="Etc/UTC" \
            --volume="$PWD/docs:/srv/jekyll:Z" \
            --volume="$PWD/docs/.jekyll-bundle-cache:/usr/local/bundle:Z" \
            --interactive --tty \
            jekyll/jekyll:3.8 \
            jekyll build --config _config.yml -d _site --watch
   ```

2. Check the links: 

   ```
   docker run --rm \
            --name lakefs_docs_lychee \
            --volume "$PWD:/data"\
            --volume "/tmp:/output"\
            --tty \
            lycheeverse/lychee:master \
            --exclude-file /data/docs/.lycheeignore \
            --output /output/lychee_report.md \
            --format markdown \
            /data/docs/_site
   ```

3. Review the `lychee_report.md` in your local `/tmp` folder
