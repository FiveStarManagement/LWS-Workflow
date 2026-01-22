UPDATE lws_order_state
SET status      = 'COMPLETE',
    last_step   = 'MANUAL_COMPLETE',
    last_error_summary = NULL,
    last_api_messages  = NULL,
    updated_ts  = CURRENT_TIMESTAMP
WHERE sordernum = 250471;
