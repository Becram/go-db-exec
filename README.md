# db exec


Copy the example config file and update it

```bash
cp config.yaml.example config.yaml
```

Build and copy the binary 

```bash
make deploy
```

Running the command

```bash
db-exec --config-file config.yaml --env staging-mysql -query="SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST where command = 'Query';" 
```
