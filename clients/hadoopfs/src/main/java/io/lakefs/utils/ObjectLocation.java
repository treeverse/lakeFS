package io.lakefs.utils;

import io.lakefs.Constants;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.net.URI;

public class ObjectLocation {
    private String scheme;
    private String repository;
    private String ref;
    private String path;

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     *
     * @param workingDirectory used if path is local.
     * @param path to extract information from.
     * @return lakeFS Location with repository, ref and path
     */
    @Nonnull
    static public ObjectLocation pathToObjectLocation(Path workingDirectory, Path path) {
        if (!path.isAbsolute()) {
            if (workingDirectory == null) {
                throw new IllegalArgumentException(String.format("cannot expand local path %s with null workingDirectory", path));
            }
            path = new Path(workingDirectory, path);
        }

        URI uri = path.toUri();
        ObjectLocation loc = new ObjectLocation();
        loc.setScheme(uri.getScheme());
        loc.setRepository(uri.getHost());
        // extract ref and rest of the path after removing the '/' prefix
        String s = StringUtils.trimLeadingSlash(uri.getPath());
        int i = s.indexOf(Constants.SEPARATOR);
        if (i == -1) {
            loc.setRef(s);
            loc.setPath("");
        } else {
            loc.setRef(s.substring(0, i));
            loc.setPath(s.substring(i + 1));
        }
        return loc;
    }

    /**
     * Returns Location with repository, ref and path used by lakeFS based on filesystem path.
     *
     * @param path to extract information from.
     * @return lakeFS Location with repository, ref and path
     */
    @Nonnull
    static public ObjectLocation pathToObjectLocation(Path path) {
        return pathToObjectLocation(null, path);
    }

    public static String formatPath(String scheme, String repository, String ref, String path) {
        String ret = formatPath(scheme, repository, ref);
        if (path != null) ret = ret + "/" + path;
        return ret;
    }


    public static String formatPath(String scheme, String repository, String ref) {
        return String.format("%s://%s/%s", scheme, repository, ref);
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public ObjectLocation() {
    }

    public ObjectLocation(String scheme, String repository, String ref) {
        this.scheme = scheme;
        this.repository = repository;
        this.ref = ref;
    }

    public ObjectLocation(String scheme, String repository, String ref, String path) {
        this.scheme = scheme;
        this.repository = repository;
        this.ref = ref;
        this.path = path;
    }

    public ObjectLocation clone() {
        return new ObjectLocation(scheme, repository, ref, path);
    }

    public ObjectLocation getParent() {
        if (this.path == null) {
            return null;
        }
        Path parentPath = new Path(this.path).getParent();
        if (parentPath == null) {
            return null;
        }
        return new ObjectLocation(scheme, repository, ref, parentPath.toString());
    }

    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isValidPath() {
        return !this.repository.isEmpty() &&
                !this.ref.isEmpty() &&
                !this.path.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ObjectLocation)) {
            return false;
        }

        ObjectLocation objLoc = (ObjectLocation) obj;
        return this.repository.equals(objLoc.getRepository()) &&
                this.ref.equals(objLoc.getRef()) && this.path.equals(objLoc.getPath());
    }

    /**
     * Checks if an ObjectLocation is on the same branch.
     *
     * @param otherObjLoc the objectLocation to compare
     * @return true if the object location is on same branch, false otherwise
     */
    public boolean onSameBranch(ObjectLocation otherObjLoc) {
        return this.scheme.equals(otherObjLoc.getScheme()) &&
                this.repository.equals(otherObjLoc.getRepository()) &&
                this.ref.equals(otherObjLoc.getRef());
    }

    @Override
    public String toString() {
        return formatPath(scheme, repository, ref, path);
    }

    public String toRefString() {
        return formatPath(scheme, repository, ref);
    }

    public ObjectLocation toDirectory() {
        return new ObjectLocation(scheme, repository, ref, StringUtils.addLeadingSlash(path));
    }

    public Path toFSPath() {
        return new Path(this.toString());
    }
}
