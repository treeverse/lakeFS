-- auth schema, containing information about lakeFS authentication and authorization
CREATE TABLE IF NOT EXISTS users (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,

    CONSTRAINT users_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS groups (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,

    CONSTRAINT groups_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS roles (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,

    CONSTRAINT roles_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS policies (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,
    permission text NOT NULL,
    arn text NOT NULL,

    CONSTRAINT policies_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS user_groups (
    user_id  integer REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    group_id integer REFERENCES groups (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, group_id)
);
CREATE INDEX idx_user_groups_user_id ON user_groups (user_id); -- list groups by user
CREATE INDEX idx_user_groups_group_id ON user_groups (group_id); -- list users by group


CREATE TABLE IF NOT EXISTS user_roles (
    user_id integer REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    role_id integer REFERENCES roles (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, role_id)
);
CREATE INDEX idx_user_role_user_id ON user_roles (user_id); -- list roles by user


CREATE TABLE IF NOT EXISTS group_roles (
    group_id integer REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    role_id integer REFERENCES roles (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (group_id, role_id)
);
CREATE INDEX idx_group_roles_group_id ON group_roles (group_id); -- list roles by group


CREATE TABLE IF NOT EXISTS role_policies (
    role_id integer REFERENCES roles (id) ON DELETE CASCADE NOT NULL,
    policy_id integer NOT NULL REFERENCES policies (id) ON DELETE CASCADE,

    PRIMARY KEY (role_id, policy_id)
);
CREATE INDEX idx_role_policies_role_id ON role_policies (role_id); -- list policies by role


CREATE TABLE IF NOT EXISTS credentials (
    access_key_id varchar(20) NOT NULL PRIMARY KEY,
    access_secret_key bytea NOT NULL,
    issued_date timestamptz NOT NULL,
    user_id integer REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX idx_credentials_user_id ON credentials (user_id); -- list credentials per user
