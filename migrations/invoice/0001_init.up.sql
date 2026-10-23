CREATE TABLE invoices (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  biz_order_no  VARCHAR(64)   NOT NULL,
  product_line  VARCHAR(16)   NOT NULL,
  title         VARCHAR(128)  NOT NULL,
  tax_no        VARCHAR(32)   NOT NULL,
  amount        DECIMAL(18,2) NOT NULL,
  customer_id   VARCHAR(64)   NOT NULL,
  status        VARCHAR(24)   NOT NULL,
  invoice_no    VARCHAR(64)   NULL,
  version       INT           NOT NULL DEFAULT 0,
  created_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_invoices_biz_order_no (biz_order_no),
  INDEX idx_invoices_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE commands (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  invoice_id BIGINT       NOT NULL,
  kind       VARCHAR(16)  NOT NULL COMMENT 'ISSUE, RED_FLUSH, REISSUE, SUPPLEMENT',
  status     VARCHAR(24)  NOT NULL,
  reason     VARCHAR(255) NOT NULL DEFAULT '',
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX idx_commands_invoice (invoice_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tasks (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  command_id    BIGINT      NOT NULL,
  invoice_id    BIGINT      NOT NULL,
  supplier      VARCHAR(32) NULL,
  status        VARCHAR(24) NOT NULL,
  attempt       INT         NOT NULL DEFAULT 0,
  next_retry_at DATETIME(3) NULL,
  deadline_at   DATETIME(3) NULL,
  version       INT         NOT NULL DEFAULT 0,
  INDEX idx_tasks_status_deadline (status, deadline_at),
  INDEX idx_tasks_invoice (invoice_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE state_log (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  entity      VARCHAR(16) NOT NULL,
  entity_id   BIGINT      NOT NULL,
  from_status VARCHAR(24) NOT NULL,
  to_status   VARCHAR(24) NOT NULL,
  cause       VARCHAR(64) NOT NULL,
  event_id    VARCHAR(36) NULL,
  at          DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX idx_state_log_entity (entity, entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE outbox (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id        VARCHAR(36)  NOT NULL,
  routing_key     VARCHAR(96)  NOT NULL,
  body            JSON         NOT NULL,
  status          VARCHAR(8)   NOT NULL DEFAULT 'new',
  attempts        INT          NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_outbox_event (event_id),
  INDEX idx_outbox_status_next (status, next_attempt_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE processed_events (
  event_id     VARCHAR(36) NOT NULL PRIMARY KEY,
  consumer     VARCHAR(32) NOT NULL,
  processed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
