db.getSiblingDB('admin').createUser({
  user: 'app',
  pwd: process.env.MONGO_APP_PASSWORD,
  roles: [{ role: 'readWrite', db: 'invoicehub' }],
});
