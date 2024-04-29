package main

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var driver neo4j.DriverWithContext

func startContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "memgraph/memgraph:latest",
		ExposedPorts: []string{"7687/tcp"},
		WaitingFor:   wait.ForListeningPort("7687/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return container, nil
}

func deleteEverything(ctx context.Context, driver neo4j.DriverWithContext) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "memgraph"})
	defer session.Close(ctx)

	query := "MATCH (n) DETACH DELETE n;"
	_, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
	if err != nil {
		log.Fatalf("Could not delete everything: %s", err)
	}

	return nil
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := startContainer(ctx)
	if err != nil {
		log.Fatalf("Could not setup memgraph container: %s", err)
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Fatalf("Could not terminate memgraph container: %s", err)
		}
	}()

	endpoint, err := container.Endpoint(ctx, "bolt")
	if err != nil {
		log.Fatalf("Could not get memgraph endpoint: %s", err)
	}

	driver, err = neo4j.NewDriverWithContext(endpoint, neo4j.BasicAuth("memgraph", "memgraph", ""))
	if err != nil {
		log.Fatalf("Could not create driver: %s", err)
	}
	defer driver.Close(ctx)

	// wait for memgraph to be ready
	attempts := 0
	for {
		attempts++
		err = driver.VerifyConnectivity(ctx)
		if err == nil {
			break
		}

		if attempts > 10 {
			log.Fatalf("Could not connect to memgraph: %s", err)
		}

		time.Sleep(1 * time.Second)
	}

	m.Run()
}

func Test_countAllNodes(t *testing.T) {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "memgraph"})
	defer session.Close(ctx)

	// before inserting nodes
	count, err := countAllNodes(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	// insert nodes
	nodes := []string{
		"CREATE (n:A {id: 'a-1', value:'A1'});",
		"CREATE (n:A {id: 'a-2', value:'A2'});",
		"CREATE (n:A {id: 'a-3', value:'A3'});",
		"CREATE (n:B {id: 'b-1', value:'B1'});",
		"CREATE (n:B {id: 'b-2', value:'B2'});",
		"CREATE (n:B {id: 'b-3', value:'B3'});",
		"CREATE (n:C {id: 'c-1', value:'C1'});",
		"CREATE (n:C {id: 'c-2', value:'C2'});",
		"CREATE (n:D {id: 'd-1', value:'D1'});",
	}
	for _, node := range nodes {
		_, err := neo4j.ExecuteQuery(ctx, driver, node, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			t.Fatalf("Could not insert node: %s", err)
		}
	}

	// insterted 9 nodes
	count, err = countAllNodes(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(9), count)

	// delete nodes of C label
	query := "MATCH (n:C) DETACH DELETE n;"
	_, err = neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
	if err != nil {
		t.Fatalf("Could not delete nodes: %s", err)
	}

	// deleted 2 nodes
	count, err = countAllNodes(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(7), count)

	// clean up
	err = deleteEverything(ctx, driver)
	if err != nil {
		t.Fatalf("Could not delete everything: %s", err)
	}
}

func Test_countAllEdges(t *testing.T) {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "memgraph"})
	defer session.Close(ctx)

	// before inserting nodes
	count, err := countAllEdges(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	// insert nodes and relationships
	nodes := []string{
		"CREATE (n:Account {id: 'alice'});",
		"CREATE (n:Account {id: 'bob'});",
		"CREATE (n:Account {id: 'charlie'});",
	}
	for _, node := range nodes {
		_, err := neo4j.ExecuteQuery(ctx, driver, node, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			t.Fatalf("Could not insert node: %s", err)
		}
	}
	edges := []string{
		"MATCH (a:Account {id: 'alice'}),(b:Account {id: 'bob'}) CREATE (a)-[r:FRIEND]->(b);",
		"MATCH (a:Account {id: 'alice'}),(c:Account {id: 'charlie'}) CREATE (a)-[r:COLLEAGUE]->(c);",
		"MATCH (b:Account {id: 'bob'}),(c:Account {id: 'charlie'}) CREATE (b)-[r:FRIEND]->(c);",
	}
	for _, edge := range edges {
		_, err := neo4j.ExecuteQuery(ctx, driver, edge, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			t.Fatalf("Could not insert edges: %s", err)
		}
	}

	// insterted 3 edges
	count, err = countAllEdges(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), count)

	query := "MATCH ()-[r:COLLEAGUE]->() DETACH DELETE r;"
	_, err = neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
	if err != nil {
		t.Fatalf("Could not delete edges: %s", err)
	}

	// deleted 1 edge
	count, err = countAllEdges(ctx, driver)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)

	// clean up
	err = deleteEverything(ctx, driver)
	if err != nil {
		t.Fatalf("Could not delete everything: %s", err)
	}
}
