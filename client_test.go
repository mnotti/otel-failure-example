package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	createColorTableQuery = `
		CREATE TABLE colors
		(
			id int,
			name LowCardinality(String),
			hex String
		)
		ENGINE = ReplacingMergeTree
		ORDER BY id
	`

	insertColorQuery = `INSERT INTO colors (id, name, hex) VALUES ($1, $2, $3)`

	getColorQuery = `
		SELECT * FROM colors
		WHERE name = $1
	`
)

type Color struct {
	Id   int    `db:"id"`
	Name string `db:"name"`
	Hex  string `db:"hex"`
}

func TestTableCreateInsertSelect(t *testing.T) {
	ctx := context.Background()
	ch, cleanup := CreateClickHouseClientAndServer(ctx, t)
	defer cleanup()
	_, err := ch.ExecContext(ctx, createColorTableQuery)
	assert.NoError(t, err)
	tx, err := ch.BeginTx(ctx, nil)
	assert.NoError(t, err)
	if err == nil {
		_, err = tx.ExecContext(ctx, insertColorQuery, 1, "red", "#FF0000")
		assert.NoError(t, err)
		tx.Commit()
	}
	var colors []Color
	err = ch.SelectContext(ctx, &colors, getColorQuery, "red")
	assert.NoError(t, err)
	expectedColors := []Color{{Name: "red", Id: 1, Hex: "#FF0000"}}
	assert.ElementsMatch(t, colors, expectedColors)
}
