-- Create "access" table
CREATE TABLE `access` (`id` uuid NOT NULL, `tenant_id` uuid NOT NULL, `actor_id` uuid NOT NULL, `trace_id` blob NOT NULL, `action` text NOT NULL, `kind` text NOT NULL, `result` text NOT NULL, `date_created` datetime NULL, `actor_tenant_id` uuid NOT NULL, `value` blob NOT NULL, `domain` integer NULL DEFAULT (0), `subjects` json NULL, `subject_count` integer NOT NULL, PRIMARY KEY (`id`));
-- Create index "access_access" to table: "access"
CREATE INDEX `access_access` ON `access` (`tenant_id`, `date_created`);
-- Create index "access_by_actor_tenant" to table: "access"
CREATE INDEX `access_by_actor_tenant` ON `access` (`actor_tenant_id`, `date_created`);
-- Create index "access_by_actor" to table: "access"
CREATE INDEX `access_by_actor` ON `access` (`actor_id`, `date_created`);
-- Create index "access_by_time" to table: "access"
CREATE INDEX `access_by_time` ON `access` (`date_created`, `id`);
-- Create "archived" table
CREATE TABLE `archived` (`id` uuid NOT NULL, `labels` json NULL, `namespace` text NOT NULL, `digest` text NOT NULL, `intent` text NOT NULL, `state` text NOT NULL, `since` integer NOT NULL, `date_updated` datetime NOT NULL, `date_created` datetime NULL, `gone` integer NOT NULL, `signature` blob NULL, `signed_by` blob NULL, PRIMARY KEY (`id`));
-- Create index "archived_blob" to table: "archived"
CREATE INDEX `archived_blob` ON `archived` (`namespace`, `digest`);
-- Create index "archived_state" to table: "archived"
CREATE INDEX `archived_state` ON `archived` (`state`);
-- Create "audit" table
CREATE TABLE `audit` (`id` uuid NOT NULL, `tenant_id` uuid NOT NULL, `actor_id` uuid NOT NULL, `trace_id` blob NOT NULL, `action` text NOT NULL, `object_id` uuid NOT NULL, `patch` blob NOT NULL, `date_created` datetime NULL, `actor_tenant_id` uuid NOT NULL, `value` blob NOT NULL, `counterpart_tenant_id` uuid NULL, `domain` integer NULL DEFAULT (0), `fields` json NULL, `digest` blob NULL, `digest_fields` json NULL, PRIMARY KEY (`id`));
-- Create index "audit_object" to table: "audit"
CREATE INDEX `audit_object` ON `audit` (`object_id`);
-- Create index "audit_trail" to table: "audit"
CREATE INDEX `audit_trail` ON `audit` (`tenant_id`, `date_created`);
-- Create index "audit_by_actor_tenant" to table: "audit"
CREATE INDEX `audit_by_actor_tenant` ON `audit` (`actor_tenant_id`, `date_created`);
-- Create index "audit_by_counterpart" to table: "audit"
CREATE INDEX `audit_by_counterpart` ON `audit` (`counterpart_tenant_id`, `date_created`);
-- Create index "audit_by_domain" to table: "audit"
CREATE INDEX `audit_by_domain` ON `audit` (`domain`, `date_created`);
-- Create index "audit_by_tenant_domain" to table: "audit"
CREATE INDEX `audit_by_tenant_domain` ON `audit` (`tenant_id`, `domain`, `date_created`);
-- Create index "audit_by_actor" to table: "audit"
CREATE INDEX `audit_by_actor` ON `audit` (`actor_id`, `date_created`);
-- Create index "audit_by_time" to table: "audit"
CREATE INDEX `audit_by_time` ON `audit` (`date_created`, `id`);
-- Create "holder" table
CREATE TABLE `holder` (`id` uuid NOT NULL, `alias` text NOT NULL, `name` text NOT NULL, `desc` text NOT NULL, `labels` json NULL, `date_updated` datetime NOT NULL, `date_erased` datetime NULL, `date_created` datetime NULL, `tenant_id` uuid NOT NULL, PRIMARY KEY (`id`), CONSTRAINT `holder_tenant_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenant` (`id`) ON DELETE NO ACTION);
-- Create index "holder_slug" to table: "holder"
CREATE UNIQUE INDEX `holder_slug` ON `holder` (`alias`, `tenant_id`) WHERE date_erased IS NULL;
-- Create index "holder_page" to table: "holder"
CREATE INDEX `holder_page` ON `holder` (`tenant_id`, `date_created`, `id`);
-- Create "issue" table
CREATE TABLE `issue` (`id` uuid NOT NULL, `rule` text NOT NULL, `source` text NOT NULL, `node` text NOT NULL, `key` text NOT NULL, `severity` text NOT NULL, `date_updated` datetime NOT NULL, `date_created` datetime NULL, `state` text NOT NULL, `level` text NOT NULL, `date_opened` datetime NULL, `date_seen` datetime NULL, `date_cleared` datetime NULL, `date_closed` datetime NULL, `close_reason` text NOT NULL, `count` integer NOT NULL, `restarts` integer NOT NULL, `attention` bool NOT NULL, `acked` bool NOT NULL, `date_acked` datetime NULL, `date_reset` datetime NULL, `suppressed` bool NOT NULL, `shelved` bool NOT NULL, `out_of_service` bool NOT NULL, PRIMARY KEY (`id`));
-- Create index "issue_page" to table: "issue"
CREATE INDEX `issue_page` ON `issue` (`date_created`, `id`);
-- Create index "issue_state" to table: "issue"
CREATE INDEX `issue_state` ON `issue` (`state`, `date_created`, `id`);
-- Create "outbox" table
CREATE TABLE `outbox` (`id` uuid NOT NULL, `tenant_id` uuid NOT NULL, `actor_id` uuid NOT NULL, `method` text NOT NULL, `by` text NOT NULL, `object_id` uuid NOT NULL, `patch` blob NOT NULL, `date_created` datetime NULL, `event` blob NULL, PRIMARY KEY (`id`));
-- Create index "outbox_queue" to table: "outbox"
CREATE INDEX `outbox_queue` ON `outbox` (`id`);
-- Create "owed" table
CREATE TABLE `owed` (`id` uuid NOT NULL, `object` uuid NOT NULL, `held_by` json NULL, `date_created` datetime NULL, PRIMARY KEY (`id`));
-- Create index "owed_owed" to table: "owed"
CREATE INDEX `owed_owed` ON `owed` (`date_created`, `id`);
-- Create "rowseal" table
CREATE TABLE `rowseal` (`id` uuid NOT NULL, `tenant_id` uuid NOT NULL, `sum` blob NOT NULL, `fields` json NULL, PRIMARY KEY (`id`));
-- Create "tenant" table
CREATE TABLE `tenant` (`id` uuid NOT NULL, `alias` text NOT NULL, `name` text NOT NULL, `desc` text NOT NULL, `labels` json NULL, `date_updated` datetime NOT NULL, `date_created` datetime NULL, PRIMARY KEY (`id`));
-- Create index "tenant_alias_key" to table: "tenant"
CREATE UNIQUE INDEX `tenant_alias_key` ON `tenant` (`alias`);
-- Create index "tenant_page" to table: "tenant"
CREATE INDEX `tenant_page` ON `tenant` (`date_created`, `id`);
