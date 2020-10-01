#If you are looking to make your first contribution, follow the steps below.

<img align="right" width="300" src="/Images/fork.jpg" alt="fork this repository" />

#### If you don't have git on your machine, [install it](https://help.github.com/articles/set-up-git/).

## Fork this repository.

Fork this repository by clicking on the fork button on the top of this page.
This will create a copy of this repository in your account.

## Clone the repository.

<img align="right" width="300" src="/Images/clone.jpg" alt="clone this repository" />

Now clone the forked repository to your machine. Go to your GitHub account, open the forked repository, click on the code button and then click the _copy to clipboard_ icon.

Open a terminal and run the following git command:

```
git clone "url you just copied"
```

where "url you just copied" (without the quotation marks) is the url to this repository (your fork of this project). See the previous steps to obtain the url.

<img align="right" width="300" src="/Images/copy-to-clipboard.jpg" alt="copy URL to clipboard" />

For example:

```
git clone https://github.com/treeverse/lakeFS
```

where `this-is-you` is your GitHub username. Here you're copying the contents of the first-contributions repository on GitHub to your computer.

## Create a branch

Change to the repository directory on your computer:

```
cd lakeFS    
```

Now create a branch using the `git checkout` command:

```
git checkout -b your-new-branch-name
```

For example:

```
git checkout -b Contributors
```

## Make necessary changes and commit those changes

```
 git add Name of the file you just created or edited
```

<img align="right" width="450" src="public/Images/git-status.png" alt="git status" />

If you go to the project directory and execute the command `git status`, you'll see there are changes.

Add those changes to the branch you just created using the `git add` command:

For example:

```
git add Contributors.md
```

Now commit those changes using the `git commit` command:

For example:

```
git commit -m "Added Contributors.md"
```

## Push changes to GitHub

Push your changes using the command `git push`:

```
git push origin your-branch-name
```

replace `your-branch-name` with the name of the branch you created earlier.

## Submit your changes for review

If you go to your repository on GitHub, you'll see a `Compare & pull request` button. Click on that button.

<img style="float: right;" src="/Images/compare-and-pull.jpg" alt="create a pull request" />

Now submit the pull request.

<img style="float: right;" src="/Images/submit-pull-request.png" alt="submit pull request" />

If your changes are correct then We'll merge all your changes into the master branch of this project. You will get a notification email once the changes have been merged.


