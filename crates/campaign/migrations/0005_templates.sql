-- Message templates with personalization tokens ({{first_name}}, {{attributes.plan}}, {{discount|10%}}).
-- Postgres is the source of truth; the campaign service mirrors them to Redis for the workers.
CREATE TABLE templates (
    channel       TEXT NOT NULL CHECK (channel IN ('push', 'email', 'sms', 'in_app')),
    key           TEXT NOT NULL CHECK (key ~ '^[a-z0-9_]{1,64}$'),
    title         TEXT,                       -- email subject / push title
    body          TEXT NOT NULL,
    hide_content  BOOLEAN NOT NULL DEFAULT false,   -- e.g. OTPs: never store the text in history
    description   TEXT,
    updated_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel, key)
);

-- What was actually sent, shown in People and Deliveries.
ALTER TABLE delivery_events ADD COLUMN title TEXT, ADD COLUMN body TEXT;

INSERT INTO templates (channel, key, title, body, hide_content, description) VALUES
 ('sms',    'otp', NULL, 'Your verification code is {{code}}. It expires in 5 minutes. Never share it with anyone.', true, 'One-time code (content hidden in history)'),
 ('email',  'otp', 'Your verification code', 'Your verification code is {{code}}. It expires in 5 minutes. If you did not request it, ignore this email.', true, 'One-time code (content hidden in history)'),
 ('push',   'otp', 'Verification code', '{{code}} is your verification code', true, 'One-time code (content hidden in history)'),
 ('in_app', 'otp', 'Verification code', '{{code}} is your verification code', true, 'One-time code (content hidden in history)'),
 ('sms',    'promo_weekly', NULL, 'Hi {{first_name|there}}, this week''s deals are live: {{discount|up to 30%}} off. Reply STOP to opt out.', false, 'Weekly promotion'),
 ('email',  'promo_weekly', '{{first_name|Hi}}, this week''s deals are here', E'Hi {{first_name|there}},\n\nThis week only: {{discount|up to 30%}} off selected items.\n\nHappy shopping!', false, 'Weekly promotion'),
 ('push',   'promo_weekly', 'This week''s deals', '{{discount|Up to 30%}} off selected items, this week only', false, 'Weekly promotion'),
 ('in_app', 'promo_weekly', 'This week''s deals', '{{discount|Up to 30%}} off selected items, this week only', false, 'Weekly promotion'),
 ('push',   'order_shipped', 'Order shipped', 'Your order {{order}} is on its way.', false, 'Transactional: order shipped'),
 ('email',  'order_shipped', 'Your order {{order}} has shipped', E'Hi {{first_name|there}},\n\nGood news: your order {{order}} is on its way.', false, 'Transactional: order shipped'),
 ('sms',    'order_shipped', NULL, 'Your order {{order}} has shipped.', false, 'Transactional: order shipped'),
 ('email',  'welcome', 'Welcome, {{first_name|friend}}!', E'Hi {{first_name|there}},\n\nThanks for joining us.', false, 'Welcome message'),
 ('sms',    'welcome', NULL, 'Welcome {{first_name|aboard}}! Thanks for joining us.', false, 'Welcome message');
