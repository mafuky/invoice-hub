const admin = db.getSiblingDB('admin');
if (!admin.auth('root', process.env.MONGO_INITDB_ROOT_PASSWORD)) quit(1);
// The image first starts a temporary standalone server for user creation.
const config = admin.runCommand({ getCmdLineOpts: 1 });
if (config.parsed?.replication?.replSet !== 'rs0') quit(1);
try {
  rs.status();
} catch (error) {
  if (error.code !== 94) throw error;
  const result = rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
  if (!result.ok) quit(1);
}
quit(admin.runCommand({ hello: 1 }).isWritablePrimary ? 0 : 1);
