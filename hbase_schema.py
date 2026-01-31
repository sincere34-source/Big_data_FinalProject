## Creating HBase Table 
create 'user_sessions', 'meta', 'activity'

## Loading session data into HBase

## inserting session 1
put 'user_sessions', 'user_000042_9999999999999',
  'meta:user_id', 'user_000042',
  'meta:session_id', 'sess_demo_001',
  'meta:start_time', '2025-03-12T14:37:22',
  'meta:end_time', '2025-03-12T14:52:41',
  'meta:duration', '919',
  'meta:conversion_status', 'converted',
  'meta:referrer', 'search_engine',
  'activity:device', '{"type":"mobile","os":"iOS","browser":"Safari"}',
  'activity:geo', '{"city":"North Michaelville","state":"WY","country":"US"}',
  'activity:viewed_products', '["prod_00123","prod_02456"]',
  'activity:cart_contents', '{"prod_00123":{"quantity":2,"price":129.99}}'

## Insert Session 2 (same user, older session)

put 'user_sessions', 'user_000042_9999999999998',
  'meta:user_id', 'user_000042',
  'meta:session_id', 'sess_demo_002',
  'meta:start_time', '2025-03-10T10:12:11',
  'meta:end_time', '2025-03-10T10:25:40',
  'meta:duration', '809',
  'meta:conversion_status', 'browsed',
  'meta:referrer', 'direct'

  ## Get all session for a specific user
  scan 'user_sessions', { STARTROW => 'user_000042_', STOPROW => 'user_000042_~' }

  ## get a single session by rowkey
  get 'user_sessions', 'user_000042_9999999999999'
