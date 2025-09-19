package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

type Person struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
}

type Config struct {
	Host     string
	Keyspace string
	Timeout  time.Duration
}

func main() {
	config := parseFlags()

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	fmt.Printf("🚀 Starting gocqlx example with ScyllaDB/Cassandra at %s\n", config.Host)
	fmt.Printf("📝 Using keyspace: %s\n", config.Keyspace)

	// Connect to cluster without keyspace
	cluster := gocql.NewCluster(config.Host)
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatalf(`❌ Failed to connect to ScyllaDB/Cassandra at %s.
Please ensure you have ScyllaDB or Cassandra running.

To start ScyllaDB with Docker:
  docker run --name scylla -p 9042:9042 -d scylladb/scylla

To start Cassandra with Docker:
  docker run --name cassandra -p 9042:9042 -d cassandra:latest

Then wait a few seconds for it to start up and run this example again.

Error: %v`, config.Host, err)
	}
	defer func() {
		fmt.Println("🔌 Closing initial session...")
		session.Close()
	}()

	if err := setupDatabase(ctx, session, config.Keyspace); err != nil {
		log.Fatalf("❌ Failed to setup database: %v", err)
	}

	// Create session with keyspace
	cluster.Keyspace = config.Keyspace
	sessionWithKeyspace, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatalf("❌ Failed to create session with keyspace: %v", err)
	}
	defer func() {
		fmt.Println("🔌 Closing keyspace session...")
		sessionWithKeyspace.Close()
	}()

	if err := demonstrateCRUD(ctx, sessionWithKeyspace); err != nil {
		log.Fatalf("❌ CRUD operations failed: %v", err)
	}

	if err := demonstrateBatchOperations(ctx, sessionWithKeyspace); err != nil {
		log.Fatalf("❌ Batch operations failed: %v", err)
	}

	fmt.Println("✅ All operations completed successfully!")
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.Host, "host", "127.0.0.1", "Cassandra/ScyllaDB host")
	flag.StringVar(&config.Keyspace, "keyspace", "gocqlx_example", "Keyspace name")
	flag.DurationVar(&config.Timeout, "timeout", 30*time.Second, "Operation timeout")

	flag.Parse()

	return config
}

func setupDatabase(ctx context.Context, session gocqlx.Session, keyspace string) error {
	fmt.Println("🔧 Setting up database...")

	// Create keyspace with context
	keyspaceStmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {
		'class': 'NetworkTopologyStrategy',
		'datacenter1': 1
	}`, keyspace)

	if err := session.ContextQuery(ctx, keyspaceStmt, nil).ExecRelease(); err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}
	fmt.Printf("✅ Keyspace '%s' created/verified\n", keyspace)

	// Create table with context
	tableStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.person (
		id int PRIMARY KEY,
		name text,
		age int
	)`, keyspace)

	if err := session.ContextQuery(ctx, tableStmt, nil).ExecRelease(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	fmt.Println("✅ Table 'person' created/verified")

	return nil
}

func demonstrateCRUD(ctx context.Context, session gocqlx.Session) error {
	fmt.Println("\n📊 Demonstrating CRUD operations...")

	// CREATE - Insert data
	fmt.Println("📝 Inserting sample data...")
	people := []Person{
		{ID: 1, Name: "Alice Johnson", Age: 28},
		{ID: 2, Name: "Bob Smith", Age: 35},
		{ID: 3, Name: "Charlie Brown", Age: 42},
	}

	for _, person := range people {
		stmt, names := qb.Insert("person").Columns("id", "name", "age").ToCql()
		if err := session.ContextQuery(ctx, stmt, names).BindStruct(person).ExecRelease(); err != nil {
			return fmt.Errorf("insert person %d: %w", person.ID, err)
		}
		fmt.Printf("   ✅ Inserted: %s (ID: %d)\n", person.Name, person.ID)
	}

	// READ - Query all data
	fmt.Println("📖 Reading all data...")
	var allPeople []Person
	stmt, names := qb.Select("person").Columns("id", "name", "age").ToCql()
	if err := session.ContextQuery(ctx, stmt, names).SelectRelease(&allPeople); err != nil {
		return fmt.Errorf("select all: %w", err)
	}

	fmt.Printf("   📋 Found %d people:\n", len(allPeople))
	for _, p := range allPeople {
		fmt.Printf("      - ID: %d, Name: %s, Age: %d\n", p.ID, p.Name, p.Age)
	}

	// UPDATE - Update a record
	fmt.Println("✏️  Updating Alice's age...")
	updateStmt, updateNames := qb.Update("person").Set("age").Where(qb.Eq("id")).ToCql()
	if err := session.ContextQuery(ctx, updateStmt, updateNames).BindMap(map[string]interface{}{
		"age": 29,
		"id":  1,
	}).ExecRelease(); err != nil {
		return fmt.Errorf("update: %w", err)
	}
	fmt.Println("   ✅ Updated Alice's age to 29")

	// READ - Verify update
	var alice Person
	selectOneStmt, selectOneNames := qb.Select("person").Columns("id", "name", "age").Where(qb.Eq("id")).ToCql()
	if err := session.ContextQuery(ctx, selectOneStmt, selectOneNames).BindMap(map[string]interface{}{
		"id": 1,
	}).GetRelease(&alice); err != nil {
		return fmt.Errorf("select one: %w", err)
	}
	fmt.Printf("   ✅ Verified: %s is now %d years old\n", alice.Name, alice.Age)

	// DELETE - Delete a record
	fmt.Println("🗑️  Deleting Charlie's record...")
	deleteStmt, deleteNames := qb.Delete("person").Where(qb.Eq("id")).ToCql()
	if err := session.ContextQuery(ctx, deleteStmt, deleteNames).BindMap(map[string]interface{}{
		"id": 3,
	}).ExecRelease(); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	fmt.Println("   ✅ Deleted Charlie's record")

	return nil
}

func demonstrateBatchOperations(ctx context.Context, session gocqlx.Session) error {
	fmt.Println("\n📦 Demonstrating batch operations...")

	// Create a batch
	batch := session.Batch(gocql.LoggedBatch)

	// Add multiple operations to the batch
	batchPeople := []Person{
		{ID: 4, Name: "Diana Prince", Age: 32},
		{ID: 5, Name: "Eve Wilson", Age: 26},
	}

	for _, person := range batchPeople {
		batch.Query("INSERT INTO person (id, name, age) VALUES (?, ?, ?)", person.ID, person.Name, person.Age)
	}

	// Execute the batch
	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute batch: %w", err)
	}
	fmt.Printf("   ✅ Batch inserted %d people\n", len(batchPeople))

	// Verify batch results
	var batchResults []Person
	stmt, names := qb.Select("person").Where(qb.In("id")).ToCql()
	ids := []int{4, 5}
	if err := session.ContextQuery(ctx, stmt, names).BindMap(map[string]interface{}{
		"id": ids,
	}).SelectRelease(&batchResults); err != nil {
		return fmt.Errorf("verify batch: %w", err)
	}

	fmt.Printf("   📋 Batch results:\n")
	for _, p := range batchResults {
		fmt.Printf("      - ID: %d, Name: %s, Age: %d\n", p.ID, p.Name, p.Age)
	}

	return nil
}
