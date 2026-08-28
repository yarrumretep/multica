ALTER TABLE autopilot_quota_period
    ADD COLUMN automated_rejection_notified_at JSONB NOT NULL DEFAULT '{}'::jsonb;
