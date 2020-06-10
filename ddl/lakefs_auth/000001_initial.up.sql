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


CREATE TABLE IF NOT EXISTS policies (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,
    action text[] NOT NULL,
    resource text NOT NULL,
    effect boolean NOT NULL,
    CONSTRAINT policies_unique_display_name UNIQUE (display_name)
);
CREATE INDEX idx_policy_actions ON policies USING GIN(action);


CREATE TABLE IF NOT EXISTS user_groups (
    user_id  integer REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    group_id integer REFERENCES groups (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, group_id)
);
CREATE INDEX idx_user_groups_user_id ON user_groups (user_id); -- list groups by user
CREATE INDEX idx_user_groups_group_id ON user_groups (group_id); -- list users by group


CREATE TABLE IF NOT EXISTS user_policies (
    user_id integer REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    policy_id integer REFERENCES policies (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, policy_id)
);
CREATE INDEX idx_user_policies_user_id ON user_policies (user_id); -- list policies by user


CREATE TABLE IF NOT EXISTS group_policies (
    group_id integer REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    policy_id integer REFERENCES policies (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (group_id, policy_id)
);
CREATE INDEX idx_group_policies_group_id ON group_policies (group_id); -- list policies by group


CREATE TABLE IF NOT EXISTS credentials (
    access_key_id varchar(20) NOT NULL PRIMARY KEY,
    access_secret_key bytea NOT NULL,
    issued_date timestamptz NOT NULL,
    user_id integer REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX idx_credentials_user_id ON credentials (user_id); -- list credentials per user
