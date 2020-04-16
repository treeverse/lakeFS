-- auth schema, containing information about lakeFS authentication and authorization
CREATE TABLE users (
    id serial NOT NULL PRIMARY KEY,
    email varchar(256) NOT NULL,
    full_name varchar(256) NOT NULL
);


CREATE TABLE applications (
    id serial NOT NULL PRIMARY KEY,
    display_name varchar(256) NOT NULL
);


CREATE TABLE groups (
    id serial NOT NULL PRIMARY KEY,
    display_name varchar(256) NOT NULL
);


CREATE TABLE roles (
    id serial NOT NULL PRIMARY KEY,
    display_name varchar(256) NOT NULL
);


CREATE TABLE user_groups (
    user_id integer REFERENCES users (id) NOT NULL,
    group_id integer REFERENCES groups (id) NOT NULL,

    PRIMARY KEY (user_id, group_id)
);


CREATE TABLE user_roles (
    user_id integer REFERENCES users (id) NOT NULL,
    role_id integer REFERENCES roles (id) NOT NULL,

    PRIMARY KEY (user_id, role_id)
);


CREATE TABLE application_groups (
    application_id integer REFERENCES applications (id) NOT NULL,
    group_id integer REFERENCES groups (id) NOT NULL,

    PRIMARY KEY (application_id, group_id)
);


CREATE TABLE application_roles (
    application_id integer REFERENCES applications (id) NOT NULL,
    role_id integer REFERENCES roles (id) NOT NULL,

    PRIMARY KEY (application_id, role_id)
);


CREATE TABLE group_roles (
    group_id integer REFERENCES groups (id) NOT NULL,
    role_id integer REFERENCES roles (id) NOT NULL,

    PRIMARY KEY (group_id, role_id)
);


CREATE TABLE policies (
    id serial NOT NULL PRIMARY KEY,
    permission varchar(256) NOT NULL,
    arn varchar(256) NOT NULL
);


CREATE TABLE role_policies (
    role_id integer REFERENCES roles (id) NOT NULL,
    policy_id integer NOT NULL REFERENCES policies (id),

    PRIMARY KEY (role_id, policy_id)
);


CREATE TABLE credentials (
    access_key_id varchar(20) NOT NULL PRIMARY KEY,
    access_secret_key bytea NOT NULL,
    credentials_type varchar(20) NOT NULL CHECK (credentials_type in ('user', 'application')),
    issued_date timestamptz NOT NULL,
                                                 
    user_id integer REFERENCES users (id),
    application_id integer REFERENCES applications (id)
);
