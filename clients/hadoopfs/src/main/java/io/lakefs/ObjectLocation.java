package io.lakefs;

class ObjectLocation {
    private String repository;
    private String ref;
    private String path;

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
        if (s.startsWith(Constants.URI_SEPARATOR)) {
            return s.substring(1);
        }
        return s;
    }
}
