package env

import (
	"os"
	"strings"

	"alfred.brave.com/event"
)

var log = event.Log

func New() {
	if os.Getenv("PROJECT_PATH") == "" {
		path, err := os.Getwd()
		if err != nil {
			log.Error("Get project current path error")
			panic(err)
		}
		paths := strings.Split(path, "/")
		parentPath := strings.Join(paths[:len(paths)-1], "/")
		if err := os.Setenv("PROJECT_PATH", parentPath); err != nil {
			log.Errorf("Set environment error: %v", err)
			panic(err)
		}
	}
	log.Infof("Enable Brave on %s", os.Getenv("PROJECT_PATH"))
}
