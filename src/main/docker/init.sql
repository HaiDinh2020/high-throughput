CREATE TABLE IF NOT EXISTS messages (
    id           VARCHAR(36)  NOT NULL PRIMARY KEY,
    payload      VARCHAR(255) NOT NULL,
    created_time BIGINT       NOT NULL
);
