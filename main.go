package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"

	"github.com/olekukonko/tablewriter"
)

func main() {
	c, err := client.NewImmuClient(client.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	lr, err := c.Login(ctx, []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(ctx, md)

	_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: `
		BEGIN TRANSACTION
			CREATE TABLE Clients (id INTEGER, name STRING, deactive BOOLEAN, PRIMARY KEY id)
			CREATE INDEX ON Clients(name)
		COMMIT
	`})
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: `
		BEGIN TRANSACTION
			CREATE TABLE Products (id INTEGER, name STRING, price INTEGER, stock INTEGER, PRIMARY KEY id)
			CREATE INDEX ON Products(name)
			CREATE INDEX ON Products(stock)
		COMMIT
	`})
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: `
		BEGIN TRANSACTION
			CREATE TABLE Orders (id INTEGER, ts INTEGER, client_id INTEGER, PRIMARY KEY id)
			CREATE INDEX ON Orders(ts)

			CREATE TABLE OrderItems (id INTEGER, order_id INTEGER, amount INTEGER, PRIMARY KEY id)
			CREATE INDEX ON OrderItems(order_id)
		COMMIT
	`})
	if err != nil {
		log.Fatal(err)
	}

	// Ingest Clients
	for i := 0; i < 100; i++ {
		_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: fmt.Sprintf("UPSERT INTO Clients (id, name) VALUES (%d, 'client%d')", i, i)})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Ingest Products
	for i := 0; i < 100; i++ {
		_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: fmt.Sprintf("UPSERT INTO Products (id, name, price, stock) VALUES (%d, 'product%d', %d, %d)", i, i, i*10, 100-i)})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Ingest Orders
	for i := 0; i < 200; i++ {
		_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: fmt.Sprintf("UPSERT INTO Orders (id, ts, client_id) VALUES (%d, NOW(), %d)", i, i%100)})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Ingest OrderItems
	for i := 0; i < 1000; i++ {
		_, err = c.SQLExec(ctx, &schema.SQLExecRequest{Sql: fmt.Sprintf("UPSERT INTO OrderItems (id, order_id, amount) VALUES (%d, %d, %d)", i, i%200, 10+i)})
		if err != nil {
			log.Fatal(err)
		}
	}

	q := "SELECT id, name, deactive FROM Clients WHERE deactive != NULL OR name < 'client20'"
	qres, err := c.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: q})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("QUERY: '%s'\r\n", q)
	renderQueryResult(qres)

	fmt.Println()

	q = "SELECT id, ts, c.name AS client_name FROM Orders INNER JOIN (Clients AS c) ON client_id = c.id WHERE id < 100"
	qres, err = c.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: q})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("QUERY: '%s'\r\n", q)
	renderQueryResult(qres)

	c.Logout(ctx)
}

func renderQueryResult(qres *schema.SQLQueryResult) {
	consoleTable := tablewriter.NewWriter(os.Stdout)

	cols := make([]string, len(qres.Columns))
	for i, c := range qres.Columns {
		cols[i] = c.Name
	}
	consoleTable.SetHeader(cols)

	for _, r := range qres.Rows {
		row := make([]string, len(r.Values))

		for i, v := range r.Values {
			row[i] = schema.RenderValue(v.Operation)
		}

		consoleTable.Append(row)
	}

	consoleTable.Render()
}
