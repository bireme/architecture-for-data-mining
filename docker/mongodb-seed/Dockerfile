FROM mongo

COPY ./data/field_validation/v5_v6.json /v5_v6.json
COPY ./data/field_validation/v38.json /v38.json
CMD mongoimport --host mongodb --db isismongodb --collection 02_domain_v5v6 --type json --file /v5_v6.json -u root -p root --authenticationDatabase=admin --drop && mongoimport --host mongodb --db isismongodb --collection 02_domain_v38 --type json --file /v38.json -u root -p root --authenticationDatabase=admin --drop