package io.lakefs;

class ObjectLocation {
    private String repository;
    private String ref;
    private String path;

    public static String formatPath(String repository, String ref, String path) {
        return String.format("%s://%s/%s/%s", Constants.URI_SCHEME,repository, ref, path);
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

    static String trimLeadingSlash(String s) {
        if (s.startsWith(Constants.SEPARATOR)) {
            return s.substring(1);
        }
        return s;
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
        return this.repository.equals(otherObjLoc.getRepository()) && this.ref.equals(otherObjLoc.getRef());
    }

    @Override
    public String toString() {
        return formatPath(repository, ref, path);
    }
}
