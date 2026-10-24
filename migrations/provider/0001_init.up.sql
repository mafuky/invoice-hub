CREATE TABLE provider_orders (
  id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_id            BIGINT      NOT NULL,
  invoice_id         BIGINT      NOT NULL,
  supplier           VARCHAR(32) NOT NULL,
  supplier_order_no  VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
  status             VARCHAR(24) NOT NULL,
  outcome            VARCHAR(24) NOT NULL DEFAULT '',
  invoice_no         VARCHAR(64) COLLATE utf8mb4_bin NULL,
  version            INT         NOT NULL DEFAULT 0,
  last_event_seq     INT         NOT NULL DEFAULT 0,
  created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_provider_orders_task (task_id),
  UNIQUE KEY uk_provider_orders_supplier_order (supplier, supplier_order_no),
  INDEX idx_provider_orders_invoice (invoice_id)
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
