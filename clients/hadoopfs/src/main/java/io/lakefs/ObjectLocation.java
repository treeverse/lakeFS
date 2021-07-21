package io.lakefs;

import org.apache.hadoop.fs.Path;

class ObjectLocation {
    private String scheme;
    private String repository;
    private String ref;
    private String path;

    public static String formatPath(String scheme, String repository, String ref, String path) {
        return String.format("%s://%s/%s/%s", scheme, repository, ref, path);
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

    static String trimLeadingSlash(String s) {
        if (s.startsWith(Constants.SEPARATOR)) {
            return s.substring(1);
        }
        return s;
    }

    static String addLeadingSlash(String s) {
        return s.isEmpty() || s.endsWith(Constants.SEPARATOR) ? s : s + Constants.SEPARATOR;
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
        return new ObjectLocation(scheme, repository, ref, addLeadingSlash(path));
    }
}
