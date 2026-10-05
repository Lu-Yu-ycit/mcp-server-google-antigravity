const path = require('path');
const crypto = require('crypto');
const os = require('os');
const fs = require('fs');
const isWindows = process.platform === 'win32';
const userHome = process.env.USERPROFILE || os.homedir();
const brokerDir = path.resolve(process.env.AGY_BROKER_DIR || path.join(process.env.LOCALAPPDATA || (isWindows ? path.join(userHome, 'AppData', 'Local') : path.join(userHome, '.local', 'state')), 'mcp-server-google-antigravity'));
const defaultWorkspace = path.resolve(process.env.AGY_WORKSPACE || process.cwd());
fs.mkdirSync(brokerDir, { recursive: true, mode: 0o700 });
try { fs.chmodSync(brokerDir, 0o700); } catch (_) {}
const normalizedHome = isWindows ? userHome.toLowerCase() : userHome;
const normalizedBrokerDir = isWindows ? brokerDir.toLowerCase() : brokerDir;
const suffix = crypto.createHash('sha256').update(normalizedHome + '\0' + normalizedBrokerDir).digest('hex').slice(0, 16);
const endpoint = isWindows ? '\\\\.\\pipe\\mcp-server-google-antigravity-' + suffix : path.join(brokerDir, 'broker.sock');
const secretFile = path.join(brokerDir, 'broker.secret');
function loadBrokerSecret() {
  fs.mkdirSync(brokerDir, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(brokerDir, 0o700); } catch (_) {}
  try { return fs.readFileSync(secretFile, 'utf8').trim(); } catch (_) {}
  const secret = crypto.randomBytes(32).toString('hex');
  try { fs.writeFileSync(secretFile, secret, { encoding: 'utf8', flag: 'wx', mode: 0o600 }); }
  catch (error) { if (error.code !== 'EEXIST') throw error; return fs.readFileSync(secretFile, 'utf8').trim(); }
  try { if (!isWindows) fs.chmodSync(secretFile, 0o600); } catch (_) {}
  return secret;
}
const brokerSecret = loadBrokerSecret();
module.exports = { brokerDir, endpoint, userHome, defaultWorkspace, isWindows, brokerSecret, version: require('./package.json').version };
