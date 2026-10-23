// Package migrations embeds the SQL schema files, one directory per MySQL database.
package migrations

import "embed"

//go:embed invoice/*.sql provider/*.sql settlement/*.sql
var FS embed.FS
