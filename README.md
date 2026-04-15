# db-exec

A CLI tool for executing SQL queries and running diagnostic reports against PostgreSQL and MySQL databases over an SSH tunnel (bastion/jump host). No direct database port exposure needed.

## Features

- Connects to databases through an SSH bastion host
- Supports PostgreSQL and MySQL
- Clean tabular output with configurable cell width truncation
- PostgreSQL meta-commands (`\dt`, `\d tablename`, `\l`, etc.)
- Write query confirmation prompt (UPDATE, DELETE, DROP, etc.)
- Built-in diagnostic reports (table stats, bloat, index usage, slow queries, and more)
- Version flag with build-time version injection

## Installation

**Requirements:** Go 1.22+

```bash
git clone https://github.com/Becram/go-db-exec.git
cd go-db-exec
sudo make deploy
```

`make deploy` builds the binary and installs it to `/usr/local/bin/db-exec`.

Verify:
```bash
db-exec --version
```

## Configuration

Copy the example config and fill in your credentials:

```bash
cp config.yaml.example config.yaml
chmod 600 config.yaml
```

```yaml
database:
  production:
    engine: postgres          # postgres or mysql
    host: "db.example.com"
    port: "5432"
    name: "mydb"
    user: "dbuser"
    password: "secret"
    jump_host: "bastion.example.com"
    jump_port: "22"
    jump_user: "ec2-user"
    jump_private_key: "/home/user/.ssh/id_rsa"

  staging-mysql:
    engine: mysql
    host: "mysql.internal"
    port: "3306"
    name: "appdb"
    user: "dbuser"
    password: "secret"
    jump_host: "bastion.example.com"
    jump_port: "22"
    jump_user: "ec2-user"
    jump_private_key: "/home/user/.ssh/id_rsa"
```

Multiple environments can be defined in the same file and selected with `--env`.

## Usage

```
db-exec [flags]

Flags:
  --env             Environment name from config file (required)
  --config-file     Path to config YAML file (required)
  --query           SQL query or psql meta-command to execute
  --report          Run a diagnostic report (see Reports section)
  --database        Override the database name from config
  --max-width       Max display width per cell, default 50 (0 = no truncation)
  --slow-threshold  Seconds threshold for slow-queries report, default 5
  --version         Print version and exit
```

`--query` and `--report` are mutually exclusive. One of them is required.

## Running Queries

```bash
db-exec --env production --config-file config.yaml \
  --query "SELECT id, name, email FROM users LIMIT 10"
```

Output:

```
bastion: bastion.example.com  db host: db.example.com  database: mydb  user: dbuser
+----+----------+----------------------+
| id | name     | email                |
+----+----------+----------------------+
| 1  | Alice    | alice@example.com    |
| 2  | Bob      | bob@example.co...    |
+----+----------+----------------------+
(2 rows)
```

### Adjust cell width

```bash
# show more content per cell
db-exec ... --query "..." --max-width 80

# no truncation
db-exec ... --query "..." --max-width 0
```

### Override database

```bash
db-exec --env production --config-file config.yaml \
  --database other_db --query "SELECT 1"
```

### Write query confirmation

Mutating queries (INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, CREATE, REPLACE) require explicit confirmation before execution:

```
Warning: this is a write operation:

  DELETE FROM sessions WHERE expires_at < now()

Type 'yes' to confirm:
```

## PostgreSQL Meta-commands

```bash
db-exec --env production --config-file config.yaml --query '\dt'
```

| Command | Description |
|---|---|
| `\l` | List databases |
| `\dt` or `\dt+` | List tables with owner and size |
| `\d tablename` | Describe table columns |
| `\dn` or `\dn+` | List schemas |
| `\du` or `\dg` | List roles |
| `\di` | List indexes |

## Diagnostic Reports

Run predefined performance and status reports without writing SQL.

```bash
db-exec --env production --config-file config.yaml --report <type>
```

| Report | Description |
|---|---|
| `table-stats` | Sequential vs index scans, rows read/fetched, inserts/updates/deletes per table |
| `write-history` | Cumulative inserts, updates, deletes, and HOT updates per table since last stats reset |
| `index-usage` | Index scan counts and tuple reads; flags unused indexes |
| `cache-hit` | Buffer cache hit ratio per table (high ratio = good, low = disk-bound) |
| `bloat` | Live vs dead tuple counts, dead ratio, last vacuum and analyze times |
| `table-size` | Table size, index size, and total size per table, sorted largest first |
| `connections` | Active connections grouped by state and user |
| `locks` | Queries currently blocked waiting on a lock, with the blocking query |
| `slow-queries` | Active queries running longer than `--slow-threshold` seconds (default 5s) |
| `all` | Runs all reports above in sequence |

### Examples

```bash
# find tables with high dead tuple counts (candidates for VACUUM)
db-exec --env production --config-file config.yaml --report bloat

# find unused indexes (candidates for removal)
db-exec --env production --config-file config.yaml --report index-usage

# find queries running longer than 30 seconds
db-exec --env production --config-file config.yaml \
  --report slow-queries --slow-threshold 30

# full health check
db-exec --env production --config-file config.yaml --report all
```

## Building

```bash
# build binary (version from git tag)
make build

# build and install to /usr/local/bin
make deploy
```

Version is injected at build time from the latest git tag:

```bash
git tag v1.0.0
make build
db-exec --version   # db-exec version v1.0.0
```
