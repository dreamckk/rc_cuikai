CREATE TABLE supplier_config (
    id          VARCHAR(64)  NOT NULL PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    url         VARCHAR(512) NOT NULL,
    headers     JSON         NOT NULL DEFAULT ('{}'),
    body_template TEXT        NOT NULL DEFAULT ('{}'),
    timeout_ms  INT          NOT NULL DEFAULT 5000,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE notification_log (
    id          VARCHAR(64)  NOT NULL PRIMARY KEY,
    supplier_id VARCHAR(64)  NOT NULL,
    payload     JSON         NOT NULL,
    status      ENUM('PENDING','DELIVERED','FAILED','DEAD') NOT NULL DEFAULT 'PENDING',
    attempts    INT          NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_status (status)
);

INSERT INTO supplier_config (id, name, url, headers, body_template, timeout_ms) VALUES
('ad_system_a', '广告系统A', 'https://httpbin.org/post',
 '{"Content-Type":"application/json","X-Api-Key":"test-key"}',
 '{"uid":"${userId}","event":"${event}"}',
 5000);
