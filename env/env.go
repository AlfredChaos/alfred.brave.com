package env

import (
	"os"
	"os/exec"

	"alfred.brave.com/event"
)

var log = event.Log
var Environment map[string]string

func New() {
	if os.Getenv("PROJECT_PATH") == "" {
		config_env := exec.Command("export", "PROJECT_PATH=${pwd}")
		if err := config_env.Start(); err != nil {
			log.Error("Get environment error")
			panic(err)
		}
	}
	Environment["PROJECT_PATH"] = os.Getenv("PROJECT_PATH")
	log.Infof("Enable Brave on %s", os.Getenv("PROJECT_PATH"))
}
