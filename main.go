package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	sshTunnel "github.com/elliotchance/sshtunnel"
	"gopkg.in/yaml.v3"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var version = "dev"

type DatabaseConfig struct {
	Engine         string `yaml:"engine"`
	Host           string `yaml:"host"`
	Port           string `yaml:"port"`
	Name           string `yaml:"name"`
	User           string `yaml:"user"`
	Password       string `yaml:"password"`
	JumpHost       string `yaml:"jump_host"`
	JumpPort       string `yaml:"jump_port"`
	JumpUser       string `yaml:"jump_user"`
	JumpPrivateKey string `yaml:"jump_private_key"`
}

type Config struct {
	Database map[string]DatabaseConfig `yaml:"database"`
}

// reportSection is a named SQL query shown as one block in a report.
type reportSection struct {
	title string
	sql   string
}

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	env := flag.String("env", "", "Environment to use")
	query := flag.String("query", "", "SQL query or psql meta-command to execute")
	report := flag.String("report", "", "Run a diagnostic report: table-stats, write-history, index-usage, cache-hit, bloat, table-size, connections, locks, slow-queries, all")
	configFile := flag.String("config-file", "", "Config file for the database credentials")
	db := flag.String("database", "", "Override the db-name from config file")
	maxWidth := flag.Int("max-width", 50, "Max display width per cell (0 = no truncation)")
	slowThreshold := flag.Int("slow-threshold", 5, "Seconds threshold for slow-queries report")
	timeout := flag.Int("timeout", 300, "Query timeout in seconds (default 300s for reports, use lower value for ad-hoc queries)")
	since := flag.String("since", "4 hours", "Time window for reports that support it (e.g. '4 hours', '30 minutes', '1 day')")
	flag.Parse()

	if *showVersion {
		fmt.Println("db-exec version", version)
		os.Exit(0)
	}

	if *query == "" && *report == "" {
		log.Fatal("either --query or --report is required")
	}
	if *query != "" && *report != "" {
		log.Fatal("--query and --report are mutually exclusive")
	}

	data, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %q", err)
	}

	var config Config
	if err = yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("Error parsing config file: %q", err)
	}

	dbConfig, exists := config.Database[*env]
	if !exists {
		log.Fatalf("Environment %q not found in config file", *env)
	}

	if *db != "" {
		dbConfig.Name = *db
	}

	jHost := fmt.Sprintf("%s@%s", dbConfig.JumpUser, dbConfig.JumpHost)
	sqlServer := fmt.Sprintf("%s:%s", dbConfig.Host, dbConfig.Port)

	tunnel, err := sshTunnel.NewSSHTunnel(
		jHost,
		sshTunnel.PrivateKeyFile(dbConfig.JumpPrivateKey),
		sqlServer,
		"0",
	)
	if err != nil {
		log.Fatalf("error creating tunnel: %v", err)
	}

	fmt.Printf("\033[1;34mbastion:\033[0m %s  \033[1;34mdb host:\033[0m %s  \033[1;34mdatabase:\033[0m %s  \033[1;34muser:\033[0m %s\n",
		dbConfig.JumpHost, dbConfig.Host, dbConfig.Name, dbConfig.User)

	listener, err := tunnel.Listen()
	if err != nil {
		log.Fatalf("error starting tunnel listener: %v", err)
	}
	tunnelPort := listener.Addr().(*net.TCPAddr).Port
	go tunnel.Serve(listener)
	defer tunnel.Close()

	conn := openDB(dbConfig, tunnelPort, *timeout)
	defer conn.Close()

	if *report != "" {
		runReport(conn, *report, *slowThreshold, *maxWidth, *timeout, *since)
	} else {
		execQuery(conn, dbConfig.Engine, *query, *maxWidth, *timeout)
	}
}

// openDB opens and pings the database connection through the tunnel.
func openDB(cfg DatabaseConfig, tunnelPort, timeout int) *sql.DB {
	var connStr string
	if cfg.Engine == "mysql" {
		connStr = fmt.Sprintf("%s:%s@tcp(localhost:%d)/%s", cfg.User, cfg.Password, tunnelPort, cfg.Name)
	} else {
		connStr = fmt.Sprintf("host=localhost port=%d dbname=%s user=%s password=%s sslmode=disable", tunnelPort, cfg.Name, cfg.User, cfg.Password)
	}

	db, err := sql.Open(cfg.Engine, connStr)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}
	return db
}

// --- Report engine ---------------------------------------------------------

var reportOrder = []string{
	"table-stats",
	"write-history",
	"index-usage",
	"cache-hit",
	"bloat",
	"table-size",
	"connections",
	"locks",
	"slow-queries",
}

func resolveReport(reportType string, slowThreshold int, since string) ([]reportSection, error) {
	if reportType == "mysql" {
		return nil, fmt.Errorf("reports are only supported for PostgreSQL")
	}

	all := map[string]reportSection{
		"table-stats": {
			title: "Table Read/Write Statistics (cumulative since stats reset)",
			sql: `
				SELECT relname AS table,
				       seq_scan, seq_tup_read AS seq_rows_read,
				       idx_scan, idx_tup_fetch AS idx_rows_fetched,
				       n_tup_ins AS inserts, n_tup_upd AS updates, n_tup_del AS deletes
				FROM pg_stat_user_tables
				ORDER BY seq_scan + idx_scan DESC`,
		},
		"write-history": {
			title: "Historical Write Activity (cumulative since stats reset)",
			sql: `
				SELECT relname AS table,
				       n_tup_ins AS inserts, n_tup_upd AS updates,
				       n_tup_del AS deletes, n_tup_hot_upd AS hot_updates,
				       pg_stat_get_db_stat_reset_time(oid::oid) AS stats_reset
				FROM pg_stat_user_tables
				JOIN pg_database ON pg_database.datname = current_database()
				ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC`,
		},
		"index-usage": {
			title: "Index Usage (cumulative since stats reset)",
			sql: `
				SELECT relname AS table, indexrelname AS index,
				       idx_scan AS scans, idx_tup_read AS tuples_read, idx_tup_fetch AS tuples_fetched,
				       CASE WHEN idx_scan = 0 THEN 'UNUSED' ELSE '' END AS warning
				FROM pg_stat_user_indexes
				ORDER BY idx_scan ASC, relname`,
		},
		"cache-hit": {
			title: "Buffer Cache Hit Ratio (cumulative since stats reset)",
			sql: `
				SELECT datname AS database,
				       blks_hit AS cache_hits,
				       blks_read AS disk_reads,
				       round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 2)::text || '%' AS cache_hit_ratio
				FROM pg_stat_database
				WHERE datname = current_database()`,
		},
		"bloat": {
			title: fmt.Sprintf("Table Bloat — vacuumed/analyzed in last %s", since),
			sql: fmt.Sprintf(`
				SELECT relname AS table,
				       n_live_tup AS live_rows, n_dead_tup AS dead_rows,
				       CASE WHEN n_live_tup = 0 THEN 'N/A'
				            ELSE round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 2)::text || '%%'
				       END AS dead_ratio,
				       last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
				FROM pg_stat_user_tables
				WHERE last_autovacuum > now() - interval '%s'
				   OR last_vacuum     > now() - interval '%s'
				   OR last_autoanalyze > now() - interval '%s'
				   OR last_analyze    > now() - interval '%s'
				ORDER BY n_dead_tup DESC`, since, since, since, since),
		},
		"table-size": {
			title: "Table Sizes (current)",
			sql: `
				SELECT relname AS table,
				       pg_size_pretty(pg_table_size(relid)) AS table_size,
				       pg_size_pretty(pg_indexes_size(relid)) AS index_size,
				       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
				       pg_total_relation_size(relid) AS total_bytes
				FROM pg_stat_user_tables
				ORDER BY total_bytes DESC`,
		},
		"connections": {
			title: fmt.Sprintf("Active Connections — state changed in last %s", since),
			sql: fmt.Sprintf(`
				SELECT state, usename AS user, count(*) AS count,
				       max(now() - state_change)::text AS longest_wait
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid()
				  AND state_change > now() - interval '%s'
				GROUP BY state, usename
				ORDER BY count DESC`, since),
		},
		"locks": {
			title: "Lock Waits (current)",
			sql: `
				SELECT blocked.pid AS blocked_pid,
				       blocked_activity.usename AS blocked_user,
				       blocking.pid AS blocking_pid,
				       blocking_activity.usename AS blocking_user,
				       blocked_activity.query AS blocked_query,
				       blocking_activity.query AS blocking_query
				FROM pg_locks blocked
				JOIN pg_stat_activity blocked_activity ON blocked.pid = blocked_activity.pid
				JOIN pg_locks blocking ON blocking.transactionid = blocked.transactionid
				  AND blocking.pid != blocked.pid
				JOIN pg_stat_activity blocking_activity ON blocking.pid = blocking_activity.pid
				WHERE NOT blocked.granted`,
		},
		"slow-queries": {
			title: fmt.Sprintf("Slow Queries — started in last %s, running > %ds", since, slowThreshold),
			sql: fmt.Sprintf(`
				SELECT pid, now() - query_start AS duration, state, query
				FROM pg_stat_activity
				WHERE state != 'idle'
				  AND query_start IS NOT NULL
				  AND query_start > now() - interval '%s'
				  AND now() - query_start > interval '%d seconds'
				ORDER BY duration DESC`, since, slowThreshold),
		},
	}

	if reportType == "all" {
		var sections []reportSection
		for _, key := range reportOrder {
			sections = append(sections, all[key])
		}
		return sections, nil
	}

	s, ok := all[reportType]
	if !ok {
		return nil, fmt.Errorf("unknown report %q — available: %s, all", reportType, strings.Join(reportOrder, ", "))
	}
	return []reportSection{s}, nil
}

func runReport(db *sql.DB, reportType string, slowThreshold, maxWidth, timeout int, since string) {
	sections, err := resolveReport(reportType, slowThreshold, since)
	if err != nil {
		log.Fatal(err)
	}

	for i, section := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("\033[1;36m=== %s ===\033[0m\n", section.title)
		printQueryResults(db, strings.TrimSpace(section.sql), maxWidth, timeout)
	}
}

// --- Query execution -------------------------------------------------------

var writeKeywords = []string{"INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "REPLACE"}

func isWriteQuery(qry string) bool {
	first := strings.ToUpper(strings.Fields(qry)[0])
	for _, kw := range writeKeywords {
		if first == kw {
			return true
		}
	}
	return false
}

func translateMetaCommand(cmd, engine string) (string, error) {
	if engine == "mysql" {
		return "", fmt.Errorf("meta-commands are only supported for PostgreSQL")
	}

	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty meta-command")
	}

	switch parts[0] {
	case `\l`, `\list`:
		return `SELECT datname AS database, pg_catalog.pg_get_userbyid(datdba) AS owner, pg_encoding_to_char(encoding) AS encoding FROM pg_database ORDER BY datname`, nil
	case `\dt`, `\dt+`:
		return `SELECT schemaname AS schema, tablename AS table, tableowner AS owner, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY schemaname, tablename`, nil
	case `\dn`, `\dn+`:
		return `SELECT schema_name AS schema, schema_owner AS owner FROM information_schema.schemata ORDER BY schema_name`, nil
	case `\du`, `\dg`:
		return `SELECT rolname AS role, rolsuper AS superuser, rolcreatedb AS createdb, rolcreaterole AS createrole FROM pg_roles ORDER BY rolname`, nil
	case `\di`:
		return `SELECT schemaname AS schema, tablename AS table, indexname AS index, indexdef AS definition FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY tablename, indexname`, nil
	case `\d`:
		if len(parts) < 2 {
			return "", fmt.Errorf(`\d requires a table name, e.g. \d tablename`)
		}
		return fmt.Sprintf(`SELECT column_name AS column, data_type AS type, character_maximum_length AS max_length, is_nullable AS nullable, column_default AS default FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position`, parts[1]), nil
	default:
		return "", fmt.Errorf("unsupported meta-command %q — supported: \\l, \\dt, \\dt+, \\dn, \\du, \\di, \\d <table>", parts[0])
	}
}

func execQuery(db *sql.DB, engine, qry string, maxWidth, timeout int) {
	if strings.HasPrefix(qry, `\`) {
		translated, err := translateMetaCommand(qry, engine)
		if err != nil {
			log.Fatal(err)
		}
		qry = translated
	}

	if isWriteQuery(qry) {
		fmt.Printf("\033[1;33mWarning:\033[0m this is a write operation:\n\n  %s\n\nType 'yes' to confirm: ", qry)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		if strings.TrimSpace(scanner.Text()) != "yes" {
			fmt.Println("Aborted.")
			return
		}
	}

	printQueryResults(db, qry, maxWidth, timeout)
}

// --- Rendering -------------------------------------------------------------

func printQueryResults(db *sql.DB, qry string, maxWidth, timeout int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, qry)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting column names: %v\n", err)
	}

	raw := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for i := range raw {
		dest[i] = &raw[i]
	}

	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	truncate := func(s string) string {
		if maxWidth > 0 && len(s) > maxWidth {
			return s[:maxWidth-3] + "..."
		}
		return s
	}

	var data [][]string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			log.Fatal(err)
		}
		row := make([]string, len(columns))
		for i, val := range raw {
			if b, ok := val.([]byte); ok {
				row[i] = truncate(string(b))
			} else {
				row[i] = truncate(fmt.Sprintf("%v", val))
			}
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v\n", err)
	}

	sep := "+"
	for _, w := range widths {
		sep += strings.Repeat("-", w+2) + "+"
	}

	printRow := func(vals []string) {
		fmt.Print("|")
		for i, v := range vals {
			fmt.Printf(" %-*s |", widths[i], v)
		}
		fmt.Println()
	}

	fmt.Println(sep)
	printRow(columns)
	fmt.Println(sep)
	for _, row := range data {
		printRow(row)
	}
	fmt.Println(sep)
	fmt.Printf("(%d row", len(data))
	if len(data) != 1 {
		fmt.Print("s")
	}
	fmt.Println(")")
}
