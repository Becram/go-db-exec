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

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	env := flag.String("env", "", "Environment to use")
	query := flag.String("query", "", "Query to execute (required)")
	configFile := flag.String("config-file", "", "config file for the database credentials")
	db := flag.String("database", "", "override the db-name from config file")
	maxWidth := flag.Int("max-width", 50, "max display width per cell (0 = no truncation)")
	flag.Parse()

	if *showVersion {
		fmt.Println("db-exec version", version)
		os.Exit(0)
	}

	if *query == "" {
		log.Fatal("--query is required")
	}

	// Read the YAML file
	data, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %q", err)
	}

	// Parse the YAML file
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error parsing config file: %q", err)
	}

	// Get the database configuration for the specified environment
	dbConfig, exists := config.Database[*env]
	if !exists {
		log.Fatalf("Environment %q not found in config file", *env)
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

	// Listen first to bind the port synchronously, then serve in the background.
	// This avoids a time.Sleep race where the port might not be ready yet.
	listener, err := tunnel.Listen()
	if err != nil {
		log.Fatalf("error starting tunnel listener: %v", err)
	}
	tunnelPort := listener.Addr().(*net.TCPAddr).Port
	go tunnel.Serve(listener)
	defer tunnel.Close()

	if *db != "" {
		dbConfig.Name = *db
	}

	execQuery(dbConfig, tunnelPort, *query, *maxWidth)

}

var writeKeywords = []string{"INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "REPLACE"}

// isWriteQuery returns true if the query starts with a mutating SQL keyword.
func isWriteQuery(qry string) bool {
	first := strings.ToUpper(strings.Fields(qry)[0])
	for _, kw := range writeKeywords {
		if first == kw {
			return true
		}
	}
	return false
}

// translateMetaCommand maps psql meta-commands to equivalent SQL queries.
// Returns an error if the command is unknown or not supported for the engine.
func translateMetaCommand(cmd, engine string) (string, error) {
	if engine == "mysql" {
		return "", fmt.Errorf("meta-commands are only supported for PostgreSQL")
	}

	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty meta-command")
	}

	base := parts[0]

	switch base {
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
		table := parts[1]
		return fmt.Sprintf(`SELECT column_name AS column, data_type AS type, character_maximum_length AS max_length, is_nullable AS nullable, column_default AS default FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position`, table), nil

	default:
		return "", fmt.Errorf("unsupported meta-command %q — supported: \\l, \\dt, \\dt+, \\dn, \\du, \\di, \\d <table>", base)
	}
}

func execQuery(cfg DatabaseConfig, tunnelPort int, qry string, maxWidth int) {
	var connStr string
	if cfg.Engine == "mysql" {
		connStr = fmt.Sprintf("%s:%s@tcp(localhost:%d)/%s", cfg.User, cfg.Password, tunnelPort, cfg.Name)
	} else {
		connStr = fmt.Sprintf("host=localhost port=%d dbname=%s user=%s password=%s sslmode=disable", tunnelPort, cfg.Name, cfg.User, cfg.Password)
	}

	if strings.HasPrefix(qry, `\`) {
		translated, err := translateMetaCommand(qry, cfg.Engine)
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

	db, err := sql.Open(cfg.Engine, connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, qry)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting column names: %v\n", err)
	}

	// Buffer all rows so we can compute column widths before printing.
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

	// Build separator line: +-------+-------+
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
