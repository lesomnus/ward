-- Create "checkpoint" table
CREATE TABLE `checkpoint` (`id` uuid NOT NULL, `date_alive` datetime NULL, `run` text NOT NULL, `date_updated` datetime NOT NULL, `date_created` datetime NULL, PRIMARY KEY (`id`));
-- Create "issueevent" table
CREATE TABLE `issueevent` (`id` uuid NOT NULL, `cursor` integer NOT NULL, `issue_id` blob NOT NULL, `event` text NOT NULL, `date_occurred` datetime NULL, `comment` text NOT NULL, `date_created` datetime NULL, `rule` text NOT NULL, `source` text NOT NULL, `node` text NOT NULL, `key` text NOT NULL, `severity` text NOT NULL, `level` text NOT NULL, `state` text NOT NULL, `date_opened` datetime NULL, `date_seen` datetime NULL, `date_closed` datetime NULL, `close_reason` text NOT NULL, `count` integer NOT NULL, `restarts` integer NOT NULL, `attention` bool NOT NULL, `acked` bool NOT NULL, `suppressed` bool NOT NULL, `shelved` bool NOT NULL, `out_of_service` bool NOT NULL, PRIMARY KEY (`id`));
-- Create index "issueevent_cursor_key" to table: "issueevent"
CREATE UNIQUE INDEX `issueevent_cursor_key` ON `issueevent` (`cursor`);
-- Create index "issueevent_page" to table: "issueevent"
CREATE INDEX `issueevent_page` ON `issueevent` (`cursor`, `id`);
-- Create index "issueevent_issue" to table: "issueevent"
CREATE INDEX `issueevent_issue` ON `issueevent` (`issue_id`, `cursor`);
-- Create "override" table
CREATE TABLE `override` (`id` uuid NOT NULL, `rule` text NOT NULL, `node` text NOT NULL, `key` text NOT NULL, `kind` text NOT NULL, `date_unshelve` datetime NULL, `date_updated` datetime NOT NULL, `date_created` datetime NULL, `comment` text NOT NULL, PRIMARY KEY (`id`));
-- Create index "override_page" to table: "override"
CREATE INDEX `override_page` ON `override` (`date_created`, `id`);
