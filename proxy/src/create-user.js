#!/usr/bin/env node
import { createUser } from './db.js';
import { hashPassword, generateToken } from './auth.js';
import readline from 'readline';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function question(prompt) {
  return new Promise((resolve) => {
    rl.question(prompt, resolve);
  });
}

async function main() {
  console.log('=== Queen Proxy - Create User ===\n');

  const username = await question('Username: ');
  
  if (!username || username.trim() === '') {
    console.error('Error: Username is required');
    process.exit(1);
  }

  const password = await question('Password: ');
  
  if (!password || password.length < 6) {
    console.error('Error: Password must be at least 6 characters');
    process.exit(1);
  }

  console.log('\nRoles:');
  console.log('  1) admin       - Full access');
  console.log('  2) read-write  - Read-write access');
  console.log('  3) read-only   - Read-only access (GET only)');
  console.log('  4) write-only  - Produce-only access (push, no read/consume)');
  
  const roleChoice = await question('\nSelect role (1-4): ');
  
  const roleMap = {
    '1': 'admin',
    '2': 'read-write',
    '3': 'read-only',
    '4': 'write-only'
  };

  const role = roleMap[roleChoice.trim()];

  if (!role) {
    console.error('Error: Invalid role selection');
    process.exit(1);
  }

  // Ask about token generation (useful for microservices)
  console.log('\nToken expiration:');
  console.log('  1) 24h    - 24 hours (default)');
  console.log('  2) 7d     - 7 days');
  console.log('  3) 30d    - 30 days');
  console.log('  4) 1y     - 1 year');
  console.log('  5) never  - No expiration (for microservices)');
  console.log('  6) skip   - Do not generate token now');

  const expiryChoice = await question('\nSelect token expiration (1-6) [1]: ');

  const expiryMap = {
    '': '24h',
    '1': '24h',
    '2': '7d',
    '3': '30d',
    '4': '1y',
    '5': 'never',
    '6': null
  };

  const tokenExpiry = expiryMap[expiryChoice.trim()];

  if (tokenExpiry === undefined) {
    console.error('Error: Invalid expiration selection');
    process.exit(1);
  }

  try {
    const passwordHash = await hashPassword(password);
    const user = await createUser(username.trim(), passwordHash, role);
    
    console.log('\n✓ User created successfully!');
    console.log(`  Username: ${user.username}`);
    console.log(`  Role: ${user.role}`);
    console.log(`  ID: ${user.id}`);

    if (tokenExpiry !== null) {
      const token = generateToken(user, tokenExpiry);
      console.log(`  Token expiry: ${tokenExpiry === 'never' ? 'Never' : tokenExpiry}`);
      console.log('\n  Bearer Token:');
      console.log(`  ${token}`);
      console.log('\n  Use this token in the Authorization header:');
      console.log(`  Authorization: Bearer ${token}`);
    }
  } catch (error) {
    if (error.code === '23505') { // Unique constraint violation
      console.error('\nError: Username already exists');
    } else {
      console.error('\nError creating user:', error.message);
    }
    process.exit(1);
  } finally {
    rl.close();
    process.exit(0);
  }
}

main();

