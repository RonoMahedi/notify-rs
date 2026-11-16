-- Lets the opt-out mirror (rebuilt into Redis after a Redis restart) read only the
-- few unsubscribed contacts instead of scanning the whole table.
CREATE INDEX IF NOT EXISTS contacts_unsubscribed_idx ON contacts (user_id) WHERE cardinality(unsubscribed) > 0;
