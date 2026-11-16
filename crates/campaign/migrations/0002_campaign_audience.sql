-- Audience snapshot taken when a campaign starts: fixes targeted count / progress,
-- and makes chunk reads an index range scan instead of a filter + sort per chunk.
CREATE TABLE campaign_audience (
    campaign_id  UUID NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL,
    PRIMARY KEY (campaign_id, user_id)
);
