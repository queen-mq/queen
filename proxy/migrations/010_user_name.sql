-- Optional human-readable name for proxy users. Existing accounts stay valid
-- with NULL; operator-created accounts require a name at the HTTP boundary.
ALTER TABLE queen_proxy.users
    ADD COLUMN IF NOT EXISTS name TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'users_name_valid'
           AND conrelid = 'queen_proxy.users'::regclass
    ) THEN
        ALTER TABLE queen_proxy.users
            ADD CONSTRAINT users_name_valid
            CHECK (name IS NULL OR (btrim(name) <> '' AND char_length(name) <= 160));
    END IF;
END;
$$;

-- Keep profile writes behind the same control-plane function boundary as
-- user creation and role management. The caller adds its own user-attributed
-- audit row; this function records the canonical data mutation.
CREATE OR REPLACE FUNCTION queen_proxy.set_user_name(
    p_user UUID,
    p_name TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id UUID;
    v_old_name  TEXT;
    v_name      TEXT;
BEGIN
    v_name := btrim(p_name);
    IF v_name IS NULL OR v_name = '' OR char_length(v_name) > 160 THEN
        RAISE EXCEPTION 'set_user_name: name must contain 1 to 160 characters';
    END IF;

    SELECT tenant_id, name
      INTO v_tenant_id, v_old_name
      FROM queen_proxy.users
     WHERE id = p_user;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'set_user_name: unknown user %', p_user;
    END IF;

    UPDATE queen_proxy.users SET name = v_name WHERE id = p_user;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, NULL, 'control_plane', p_user,
        'user_name_changed', p_user::text,
        jsonb_build_object('old_name', v_old_name, 'name', v_name)
    );
END;
$$;
