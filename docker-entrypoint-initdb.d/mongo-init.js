print('Start #################################################################');

db = db.getSiblingDB('isismongodb');

db.createUser(
  {
    user: 'isismongo',
    pwd: 'isismongo',
    roles: [{ role: 'readWrite', db: 'isismongodb' }],
  },
);

print('END #################################################################');