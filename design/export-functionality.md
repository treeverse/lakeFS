# Export

## Requirements
The functionality currently provided by the lakeFS Export utility but without a dependency on Apache Spark.
This includes:
1. Export all data from a given lakeFS reference (commit or branch) to a designated object store location.
2. Export only the diff between a specific branch and a commit reference. 
3. Success/Failure indications on the exported objects. 


## Non-Requirements
1. Support distribution across machines.


## Possible Solutions

### Spark client stand-alone

Create a standalone Docker container that runs the spark client. 

Pros:
- Utilizes the existing spark client, and gives the same functionality. 
 
Cons:
- Not a built-in command in lakeFS, therefore require some adjustments from the users.
  - This solution probably requires the greatest effort from the users.


### Using the java client
Duplicate the spark client functionality and create an Exporter class that doesn't use spark. This class can be used with the java client. 

Pros:
- An intermediate solution, that require some little adjustments from the user, but also compatible with the issue.
Cons:
- Not a built-in command in lakeFS, therefore require some adjustments from the users
- Require checking how spark is being used in the current implementation (in order to rewrite its functions). Not necessarily possible in a naive way.


### Using Rclone
[Rclone](https://rclone.org/) is a command line program to sync files and directories between cloud providers.
Users can use Rclone to copy files from lakeFS to a designated object store location, or use Rclone's sync command which is similar to export only the diff between a specific branch and a commit reference.

Pros:
- Rclone gives the functionality of exporting data from lakeFS to a designated object store location. Doesn't require any additional work. 

Cons:
- Not a built-in command in lakeFS, therefore require some adjustments from the users.
- Doesn't support success/failure indications on the exported objects.
- Require a designated object store location that doesn't contain any other data but the data associated to the lakeFS branch. That is because Rclone's sync command will delete all files that don't exist in the branch.   


### Add export functionality to lakectl

Pros:
- A built-in command in lakeFS, doesn't require adjustments from the users.

Cons:
- Require a lot of work and adjustments, may not be compatible with the issue.

### Add export functionality to lakeFS (like lakefs run)

Pros:
- A built-in command in lakeFS, doesn't require adjustments from the users.

Cons:
- Require a lot of work and adjustments, may not be compatible with the issue.
