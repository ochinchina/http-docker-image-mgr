package main

import (
	"github.com/fsouza/go-dockerclient"
)

func main() {
	endpoint := "unix:///var/run/docker.sock"
	client, err := docker.NewClient(endpoint)
	if err != nil {
		panic(err)
	}
	image_storage := NewDockerImageStorage(client)
    NewImageWeb( image_storage ).Serve()
}
