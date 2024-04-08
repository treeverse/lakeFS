---
title: 7️⃣ Work with lakeFS data locally
description: lakeFS quickstart / Bring lakeFS data to a local environment to show how lakeFS can be used for ML experiments development. 
parent: ⭐ Quickstart
nav_order: 35
next: ["Resources for learning more about lakeFS", "./learning-more-lakefs.html"]
previous: ["Using Actions and Hooks in lakeFS", "./actions-and-hooks.html"]
---

# Work with lakeFS Data Locally

When working with lakeFS, there are scenarios where we need to access and manipulate data locally. An example use case for working 
locally is machine learning model development. Machine learning model development is dynamic and iterative. To optimize this 
process, experiments need to be conducted with speed, tracking ease, and reproducibility. Localizing model data during development 
accelerates the process by enabling interactive and offline development and reducing data access latency.

We're going to use [lakectl local](../howto/local-checkouts.md) to bring a subset of our lakeFS data to a local directory within the lakeFS
container and edit an image dataset used for ML model development.

## Cloning a Subset of lakeFS Data into a Local Directory

1. In lakeFS create a new branch called `my-experiment`. You can do this through the UI or with `lakectl`:

    ```bash
    docker exec lakefs \
        lakectl branch create \
            lakefs://quickstart/my-experiment \
            --source lakefs://quickstart/main
    ```

2. Clone images from your quickstart repository into a local directory named `my_local_dir` within your container:   

    ```bash
    docker exec lakefs \
        lakectl local clone lakefs://quickstart/my-experiment/images my_local_dir
    ```

3. Verify that `my_local_dir` is linked to the correct path in your lakeFS remote: 
  
    ```bash
    docker exec lakefs \
        lakectl local list
    ```

   You should see confirmation that my_local_dir is tracking the desired lakeFS path.:    
   
   ```bash
       my_local_dir	lakefs://quickstart/my-experiment/images/	8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53
   ```

4. Verify that your local environment is up-to-date with its remote path:
    
   ```bash
    docker exec lakefs \
        lakectl local status my_local_dir
    ```
    You should get a confirmation message like this showing that there is no difference between your local environment and the lakeFS remote:

   ```bash
   diff 'local:///home/lakefs/my_local_dir' <--> 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/'...
   diff 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/' <--> 'lakefs://quickstart/my-experiment/images/'...

   No diff found.
   ```    

## Making Changes to Data Locally

1. Download a new image of an Axolotl and add it to the dataset cloned into `my_local_dir`:  

    ```bash
    curl -L https://go.lakefs.io/43ENDyS > axolotl.png
   
    docker cp axolotl.png lakefs:/home/lakefs/my_local_dir
   ```

2. Clean the dataset by removing images larger than 225 KB:
    ```bash  
    docker exec lakefs \
        find my_local_dir -type f -size +225k -delete
    ```
   
3. Check the status of your local changes compared to the lakeFS remote path:
    ```bash
    docker exec lakefs \
        lakectl local status my_local_dir
    ```
   
    You should get a confirmation message like this, showing the modifications you made locally: 
    ```bash
    diff 'local:///home/lakefs/my_local_dir' <--> 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/'...
    diff 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/' <--> 'lakefs://quickstart/my-experiment/images/'...

    ╔════════╦══════════╦═════════════════════╗
    ║ SOURCE ║ CHANGE   ║ PATH                ║
    ╠════════╬══════════╬═════════════════════╣
    ║ local  ║ modified ║ axolotl.png         ║
    ║ local  ║ removed  ║ duckdb-main-02.png  ║
    ║ local  ║ removed  ║ empty-repo-list.png ║
    ║ local  ║ removed  ║ repo-contents.png   ║
    ╚════════╩══════════╩═════════════════════╝
    ```

## Pushing Local Changes to lakeFS

Once we are done with editing the image dataset in our local environment, we will push our changes to the lakeFS remote so that 
the improved dataset is shared and versioned.

1. Commit your local changes to lakeFS: 

    ```bash
    docker exec lakefs \
        lakectl local commit \
            -m 'Deleted images larger than 225KB in size and changed the Axolotl image' my_local_dir
    ```
    
    In your branch, you should see the commit including your local changes:
   
    <img width="75%" src="{{ site.baseurl }}/assets/img/quickstart/lakectl-local-01.png" alt="A lakectl local commit to lakeFS" class="quickstart"/>

2. Compare `my-experiment` branch to the `main` branch to visualize your changes:
        
    <img width="75%" src="{{ site.baseurl }}/assets/img/quickstart/lakectl-local-02.png" alt="A comparison between a branch that includes local changes to the main branch" class="quickstart"/>

## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you.

Implement the requirement from the beginning of this quickstart *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

# Finishing Up

Once you've finished the quickstart, shut down your local environment with the following command:

```bash
docker stop lakefs
```

