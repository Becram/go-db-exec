package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	sshTunnel "github.com/elliotchance/sshtunnel"
	"gopkg.in/yaml.v3"

	"text/tabwriter"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

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
	env := flag.String("env", "", "Environment to use")
	query := flag.String("query", "SELECT * FROM pg_stat_activity WHERE state = 'active'", "Query to execute")
	configFile := flag.String("config-file", "", "config file for the database credentials")
	flag.Parse()

	// Read the YAML file
	data, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %q", err)
		os.Exit(1)
	}

	// Parse the YAML file
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error parsing config file: %q", err)
		os.Exit(1)
	}

	// Get the database configuration for the specified environment
	dbConfig, exists := config.Database[*env]
	if !exists {
		log.Fatalf("Environment %q not found in config file", *env)
		os.Exit(1)
	}

	jHost := fmt.Sprintf("%s@%s", dbConfig.JumpUser, dbConfig.JumpHost)
	sqlServer := fmt.Sprintf("%s:%s", dbConfig.Host, dbConfig.Port)

	tunnel, err := sshTunnel.NewSSHTunnel(
		// User and host of tunnel server, it will default to port 22
		// if not specified.
		jHost,

		// Pick ONE of the following authentication methods:
		sshTunnel.PrivateKeyFile(dbConfig.JumpPrivateKey), // 1. private key                          // 3. ssh-agent

		// The destination host and port of the actual server.
		sqlServer,

		// The local port you want to bind the remote port to.
		// Specifying "0" will lead to a random port.
		"0",
	)
	if err != nil {
		log.Fatalf("error creating tunnel: %v", err)
	}

	tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)

	go tunnel.Start()
	time.Sleep(100 * time.Millisecond)

	execQuery(dbConfig, tunnel.Local.Port, *query)

}

func execQuery(cfg DatabaseConfig, tunnelPort int, qry string) {
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
	defer db.Close()

	// Ping the database to verify connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	query := qry
	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting column names: %v\n", err)
	}
	// Prepare to print results in a table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	defer w.Flush()

	// Print column names
	for _, col := range columns {
		fmt.Fprintf(w, "%s\t", col)
	}
	fmt.Fprintln(w)

	// Create a slice to store the values for each row
	rawResult := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns)) // Create a slice of interface{} to hold the pointers to each column value

	for i := range rawResult {
		dest[i] = &rawResult[i] // Assign the address of the slice element to the destination
	}

	// Iterate over result rows
	for rows.Next() {
		err := rows.Scan(dest...)
		if err != nil {
			log.Fatal(err)
		}

		// Print each row
		for _, val := range rawResult {
			var v interface{}
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			fmt.Fprintf(w, "%v\t", v)
		}
		fmt.Fprintln(w)
	}
	// Check for errors during rows iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v\n", err)
	}

}
