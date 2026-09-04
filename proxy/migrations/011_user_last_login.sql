-- Last successful interactive authentication. NULL means the account has
-- never completed a login since this field was introduced.
ALTER TABLE queen_proxy.users
    ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;

-- Update the account and append the existing `login` audit event atomically.
-- Login remains best-effort from the HTTP handler: an audit/control-plane
-- outage must not invalidate a session that was already authenticated.
CREATE OR REPLACE FUNCTION queen_proxy.record_user_login(
    p_user UUID
) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id UUID;
    v_login_at  TIMESTAMPTZ;
BEGIN
    UPDATE queen_proxy.users
       SET last_login_at = now()
     WHERE id = p_user
     RETURNING tenant_id, last_login_at INTO v_tenant_id, v_login_at;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'record_user_login: unknown user %', p_user;
    END IF;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, NULL, 'user', p_user,
        'login', p_user::text, '{}'::jsonb
    );

    RETURN v_login_at;
END;
$$;
