// Package migrations embeds the SQL migrations so binaries and tests do not
// depend on the process working directory.
package migrations

import "embed"

// FS contains the ordered SQL migration files.
//
//go:embed *.sql
var FS embed.FS
