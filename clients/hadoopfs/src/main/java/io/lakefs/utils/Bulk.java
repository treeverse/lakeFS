package io.lakefs.utils;

import io.lakefs.LakeFSClient;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectStageCreation;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Pagination;
import io.lakefs.clients.api.model.PathList;

import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk lakeFS operations.
 *
 * TODO(ariels): Some LakeFSFileSystem operations could use these.
 */
public class Bulk {
    private static final Logger LOG = LoggerFactory.getLogger(Bulk.class);

    protected final LakeFSClient lakeFSClient;

    /**
     * Size of bulks to use.  This is suitable for both listing and bulk
     * deletion in the lakeFS API.
     */
    public static final int BULK_SIZE = 1000;

    public Bulk(LakeFSClient lakeFSClient) {
        this.lakeFSClient = lakeFSClient;
    }

    protected void ensureTrailingSlash(ObjectLocation loc) {
        if (!loc.getPath().endsWith("/")) {
            loc.setPath(loc.getPath() + "/");
        }
    }

    /**
     * Delete all objects beneath prefixLoc.
     */
    public void deletePrefix(ObjectLocation prefixLoc) throws ApiException {
        ObjectsApi objects = lakeFSClient.getObjects();
        ensureTrailingSlash(prefixLoc);

        String after = "";
        Pagination page;
        do {
            ObjectStatsList o =
                objects.listObjects(prefixLoc.getRepository(), prefixLoc.getRef(), false,
                                    after, BULK_SIZE, "", prefixLoc.getPath());
            page = o.getPagination();
            after = page.getNextOffset();
            if (o.getResults().isEmpty()) {
                break;
            }

            // TODO(ariels): It may be faster to delete asynchronously using
            //     BulkDeleter here.
            PathList p = new PathList();
            String lastPath = null;
            for (ObjectStats stats : o.getResults()) {
                String path = stats.getPath();
                p.addPathsItem(path);
                lastPath = path;
            }
            // BUG(ariels): Handle pathType??
            LOG.info("{}/{}: Delete objects range {}..{}",
                     prefixLoc.getRepository(), prefixLoc.getRef(), p.getPaths().get(0), lastPath);
            objects.deleteObjects(prefixLoc.getRepository(), prefixLoc.getRef(), p);
        } while (page.getHasMore());
    }

    public void copyPrefix(ObjectLocation fromLoc, ObjectLocation toLoc) throws ApiException {
        ObjectsApi objects = lakeFSClient.getObjects();
        String after = "";
        ensureTrailingSlash(fromLoc);
        ensureTrailingSlash(toLoc);
        Path listBase = new ObjectLocation(fromLoc.getScheme(), fromLoc.getRepository(), fromLoc.getRef()).toFSPath();
        Pagination page;
        do {
            ObjectStatsList o =
                objects.listObjects(fromLoc.getRepository(), fromLoc.getRef(), true,
                                    after, BULK_SIZE, "", fromLoc.getPath());
            page = o.getPagination();
            after = page.getNextOffset();

            for (ObjectStats stats : o.getResults()) {
                ObjectLocation src = ObjectLocation.pathToObjectLocation(listBase, new Path(stats.getPath()));
                ObjectLocation dst = src.clone();
                if (src.getPath().startsWith(fromLoc.getPath())) {
                    dst.setPath(toLoc.getPath() + src.getPath().substring(fromLoc.getPath().length()));
                } else {
                    throw new RuntimeException(String.format("[I] %s does not have expected prefix %s", src, fromLoc));
                }
                LOG.trace("{} -> {}", src, dst);
                // TODO(ariels): Support upcoming copy-on-staging API.
                ObjectStageCreation stage = new ObjectStageCreation()
                    .physicalAddress(stats.getPhysicalAddress())
                    .checksum(stats.getChecksum())
                    .sizeBytes(stats.getSizeBytes())
                    .mtime(stats.getMtime())
                    .metadata(stats.getMetadata())
                    .contentType(stats.getContentType());
                objects.stageObject(dst.getRepository(), dst.getRef(), dst.getPath(), stage);
            }
        } while (page.getHasMore());
    }
}
