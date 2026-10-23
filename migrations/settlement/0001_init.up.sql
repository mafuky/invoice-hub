CREATE TABLE settlements (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  period        CHAR(7)       NOT NULL,
  customer_id   VARCHAR(64) COLLATE utf8mb4_bin   NOT NULL,
  amount        DECIMAL(18,2) NOT NULL,
  status        VARCHAR(24)   NOT NULL,
  version       INT           NOT NULL DEFAULT 0,
  reject_reason VARCHAR(255)  NOT NULL DEFAULT '',
  created_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_settlements_period_customer (period, customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE settlement_items (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  settlement_id BIGINT        NOT NULL,
  invoice_id    BIGINT        NOT NULL,
  product_line  VARCHAR(16)   NOT NULL,
  amount        DECIMAL(18,2) NOT NULL,
  verified      TINYINT       NOT NULL DEFAULT 0,
  checks        JSON          NULL,
  UNIQUE KEY uk_settlement_items_invoice (invoice_id),
  INDEX idx_settlement_items_settlement (settlement_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE state_log (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  entity      VARCHAR(16) NOT NULL,
  entity_id   BIGINT      NOT NULL,
  from_status VARCHAR(24) NOT NULL,
  to_status   VARCHAR(24) NOT NULL,
  cause       VARCHAR(64) NOT NULL,
  event_id    VARCHAR(36) COLLATE utf8mb4_bin NULL,
  at          DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX idx_state_log_entity (entity, entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE outbox (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id        VARCHAR(36) COLLATE utf8mb4_bin  NOT NULL,
  routing_key     VARCHAR(96) COLLATE utf8mb4_bin  NOT NULL,
  body            JSON         NOT NULL,
  status          VARCHAR(8)   NOT NULL DEFAULT 'new',
  attempts        INT          NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_outbox_event (event_id),
  INDEX idx_outbox_status_next (status, next_attempt_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE processed_events (
  event_id     VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
  consumer     VARCHAR(32) NOT NULL,
  processed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
