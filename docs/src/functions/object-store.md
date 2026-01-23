# Object Store

```sql
-- Upload file content to NATS Object Store under a given name
SELECT nats_put_file('store', 'file_name.txt', 'file content'::bytea);

-- Download file content from NATS Object Store by name
SELECT nats_get_file('store', 'file_name.txt');

-- Delete a file from the NATS Object Store by name
SELECT nats_delete_file('store', 'file_name.txt');

-- Get metadata for a specific file in the Object Store
SELECT * FROM nats_get_file_info('store', 'file_name.txt');

-- List all files in a given NATS Object Store
SELECT * FROM nats_get_file_list('store');
```
