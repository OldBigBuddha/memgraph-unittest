package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	ctx := context.Background()
	host := fmt.Sprintf("bolt://%s:%s", "localhost", "7687")
	driver, err := neo4j.NewDriverWithContext(host, neo4j.BasicAuth("memgraph", "memgraph", ""))

	if err != nil {
		log.Fatalf("Could not create driver: %s", err)
	}
	defer driver.Close(ctx)

	// wait for memgraph to be ready
	err = waitFor(ctx, driver)
	if err != nil {
		log.Fatalf("Could not connect to memgraph: %s", err)
	}

	err = insertData(ctx, driver)
	if err != nil {
		log.Fatalf("Could not insert data into memgraph: %s", err)
	}

	nodes, err := countAllNodes(ctx, driver)
	if err != nil {
		log.Fatalf("Could not count nodes: %s", err)
	}
	slog.Info("Total nodes in the graph", "nodes", nodes)

	edges, err := countAllEdges(ctx, driver)
	if err != nil {
		log.Fatalf("Could not count edges: %s", err)
	}
	slog.Info("Total edges in the graph", "edges", edges)
}

func waitFor(ctx context.Context, driver neo4j.DriverWithContext) error {
	attemps := 0
	for {
		attemps++
		err := driver.VerifyConnectivity(ctx)
		if err != nil {
			slog.Error("Could not connect to memgraph", "attempt", attemps)
		} else {
			slog.Info("Connected to memgraph")
			break
		}

		if attemps > 100 {
			return fmt.Errorf("could not connect to memgraph even though it took %d attemps", attemps)
		}

		time.Sleep(1 * time.Second)
	}
	return nil
}

func insertData(ctx context.Context, driver neo4j.DriverWithContext) error {
	indexes := []string{
		"CREATE INDEX ON :Developer(id);",
		"CREATE INDEX ON :Technology(id);",
		"CREATE INDEX ON :Developer(name);",
		"CREATE INDEX ON :Technology(name);",
	}

	// Create developer nodes
	developer_nodes := []string{
		"CREATE (n:Developer {id: 1, name:'Andy'});",
		"CREATE (n:Developer {id: 2, name:'John'});",
		"CREATE (n:Developer {id: 3, name:'Michael'});",
	}

	// Create technology nodes
	technology_nodes := []string{
		"CREATE (n:Technology {id: 1, name:'Memgraph', description: 'Fastest graph DB in the world!', createdAt: Date()})",
		"CREATE (n:Technology {id: 2, name:'Go', description: 'Go programming language ', createdAt: Date()})",
		"CREATE (n:Technology {id: 3, name:'Docker', description: 'Docker containerization engine', createdAt: Date()})",
		"CREATE (n:Technology {id: 4, name:'Kubernetes', description: 'Kubernetes container orchestration engine', createdAt: Date()})",
		"CREATE (n:Technology {id: 5, name:'Python', description: 'Python programming language', createdAt: Date()})",
	}

	//Create relationships between developers and technologies
	relationships := []string{
		"MATCH (a:Developer {id: 1}),(b:Technology {id: 1}) CREATE (a)-[r:LOVES]->(b);",
		"MATCH (a:Developer {id: 2}),(b:Technology {id: 3}) CREATE (a)-[r:LOVES]->(b);",
		"MATCH (a:Developer {id: 3}),(b:Technology {id: 1}) CREATE (a)-[r:LOVES]->(b);",
		"MATCH (a:Developer {id: 1}),(b:Technology {id: 5}) CREATE (a)-[r:LOVES]->(b);",
		"MATCH (a:Developer {id: 2}),(b:Technology {id: 2}) CREATE (a)-[r:LOVES]->(b);",
		"MATCH (a:Developer {id: 3}),(b:Technology {id: 4}) CREATE (a)-[r:LOVES]->(b);",
	}

	//Create a simple session
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "memgraph"})
	defer session.Close(ctx)

	// Run index queries via implicit auto-commit transaction
	for _, index := range indexes {
		_, err := session.Run(ctx, index, nil)
		if err != nil {
			return err
		}
	}

	// Run developer node queries
	for _, node := range developer_nodes {
		_, err := neo4j.ExecuteQuery(ctx, driver, node, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			return err
		}
	}

	// Run technology node queries
	for _, node := range technology_nodes {
		_, err := neo4j.ExecuteQuery(ctx, driver, node, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			return err
		}
	}

	// Run relationship queries
	for _, rel := range relationships {
		_, err := neo4j.ExecuteQuery(ctx, driver, rel, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
		if err != nil {
			return err
		}
	}
	slog.Info("****** All data inserted *******")
	return nil
}

func countAllNodes(ctx context.Context, driver neo4j.DriverWithContext) (int64, error) {
	query := "MATCH (n) RETURN count(n);"
	result, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
	if err != nil {
		return 0, err
	}

	return result.Records[0].Values[0].(int64), nil
}

func countAllEdges(ctx context.Context, driver neo4j.DriverWithContext) (int64, error) {
	query := "MATCH ()-[]->() RETURN count(*);"
	result, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("memgraph"))
	if err != nil {
		return 0, err
	}

	return result.Records[0].Values[0].(int64), nil
}
