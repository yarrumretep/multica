ALTER TABLE autopilot_quota_period
    ADD COLUMN notified_thresholds JSONB NOT NULL DEFAULT '{}'::jsonb;
