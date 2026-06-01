import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: process.env.PG_PORT || 5432,
  database: process.env.PG_DB || 'postgres',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'postgres',
  ssl: process.env.PG_USE_SSL ? { rejectUnauthorized: process.env.PG_SSL_REJECT_UNAUTHORIZED === 'true' } : false,
  max: 2,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

export async function query(text, params) {
  return await pool.query(text, params);
}

export async function initDatabase() {
  await query('CREATE SCHEMA IF NOT EXISTS queen_proxy');
  
  await query(`
    CREATE TABLE IF NOT EXISTS queen_proxy.users (
      id SERIAL PRIMARY KEY,
      username VARCHAR(255) UNIQUE NOT NULL,
      password_hash VARCHAR(255) NOT NULL,
      role VARCHAR(50) NOT NULL CHECK (role IN ('admin', 'read-write', 'read-only', 'write-only')),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  
  await query('CREATE INDEX IF NOT EXISTS idx_users_username ON queen_proxy.users(username)');

  // Migration: widen the role CHECK to include 'write-only' (issue #31).
  // CREATE TABLE IF NOT EXISTS won't touch an existing table's constraint, so
  // drop and re-add it under a stable name. Postgres auto-names the inline
  // single-column check `users_role_check`, which is what we drop here.
  await query('ALTER TABLE queen_proxy.users DROP CONSTRAINT IF EXISTS users_role_check');
  await query(`ALTER TABLE queen_proxy.users ADD CONSTRAINT users_role_check CHECK (role IN ('admin', 'read-write', 'read-only', 'write-only'))`);

  // Migration: relax password_hash and add OAuth columns. All idempotent.
  await query('ALTER TABLE queen_proxy.users ALTER COLUMN password_hash DROP NOT NULL');
  await query('ALTER TABLE queen_proxy.users ADD COLUMN IF NOT EXISTS email VARCHAR(255)');
  await query('ALTER TABLE queen_proxy.users ADD COLUMN IF NOT EXISTS google_sub VARCHAR(255)');
  await query(`ALTER TABLE queen_proxy.users ADD COLUMN IF NOT EXISTS auth_provider VARCHAR(50) NOT NULL DEFAULT 'local'`);
  await query('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON queen_proxy.users(email) WHERE email IS NOT NULL');
  await query('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub ON queen_proxy.users(google_sub) WHERE google_sub IS NOT NULL');

  await query(`
    CREATE TABLE IF NOT EXISTS queen_proxy.sessions (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES queen_proxy.users(id) ON DELETE CASCADE,
      token_jti VARCHAR(255) UNIQUE NOT NULL,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  
  await query('CREATE INDEX IF NOT EXISTS idx_sessions_token_jti ON queen_proxy.sessions(token_jti)');
  await query('CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON queen_proxy.sessions(user_id)');
}

export async function getUserByUsername(username) {
  const result = await query(
    'SELECT id, username, password_hash, role, email, google_sub, auth_provider FROM queen_proxy.users WHERE username = $1',
    [username]
  );
  return result.rows[0];
}

export async function createUser(username, passwordHash, role) {
  const result = await query(
    `INSERT INTO queen_proxy.users (username, password_hash, role, auth_provider)
     VALUES ($1, $2, $3, 'local')
     RETURNING id, username, role`,
    [username, passwordHash, role]
  );
  return result.rows[0];
}

export async function getUserByGoogleSub(googleSub) {
  const result = await query(
    'SELECT id, username, password_hash, role, email, google_sub, auth_provider FROM queen_proxy.users WHERE google_sub = $1',
    [googleSub]
  );
  return result.rows[0];
}

export async function getUserByEmail(email) {
  const result = await query(
    'SELECT id, username, password_hash, role, email, google_sub, auth_provider FROM queen_proxy.users WHERE email = $1',
    [email.toLowerCase()]
  );
  return result.rows[0];
}

// Attach a Google identity to an existing local user (account linking).
export async function linkGoogleAccount(userId, googleSub, email) {
  const result = await query(
    `UPDATE queen_proxy.users
       SET google_sub = $2,
           email = COALESCE(email, $3),
           updated_at = CURRENT_TIMESTAMP
     WHERE id = $1
     RETURNING id, username, role, email, google_sub, auth_provider`,
    [userId, googleSub, email.toLowerCase()]
  );
  return result.rows[0];
}

// Auto-provision a Google-only user. Username defaults to the email.
export async function createGoogleUser({ email, googleSub, role, username }) {
  const normalizedEmail = email.toLowerCase();
  const finalUsername = (username || normalizedEmail).slice(0, 255);
  const result = await query(
    `INSERT INTO queen_proxy.users (username, password_hash, role, email, google_sub, auth_provider)
     VALUES ($1, NULL, $2, $3, $4, 'google')
     RETURNING id, username, role, email, google_sub, auth_provider`,
    [finalUsername, role, normalizedEmail, googleSub]
  );
  return result.rows[0];
}

export default pool;
