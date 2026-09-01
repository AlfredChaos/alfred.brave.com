package commands

import (
	"alfred.brave.com/event"
	"github.com/urfave/cli"
)

var log = event.Log

var Braves = []cli.Command{
	// Web service
	StartCommand,
	// Database migration tool
	MigrationCommand,
	// IM kernel service
	JokerCommand,
	// Online-status reconciliation worker
	GhostCommand,
	// Kafka persist worker
	PersistCommand,
}
