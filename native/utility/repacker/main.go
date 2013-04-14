// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var (
	outputArchive = flag.String("outputArchive", "", "The output archive file.")
)

type directory struct {
	Path string
}

func newDirectory() (d *directory, err error) {
	path, err := ioutil.TempDir("", "packer")
	if err != nil {
		return
	}

	d = &directory{
		Path: path,
	}

	return
}

func (d directory) Close() {
	err := os.RemoveAll(d.Path)
	if err != nil {
		log.Println(err)
	}
}

type archive struct {
	Path string
}

func (a archive) Extract(d directory) (err error) {
	errChan := make(chan error)

	go func() {
		args := []string{"xv", a.Path}
		cmd := exec.Command("/usr/bin/ar", args...)
		cmd.Dir = d.Path
		err = cmd.Run()
		errChan <- err
	}()

	select {
	case err = <-errChan:
		return
	case <-time.After(time.Second * 5):
		err = fmt.Errorf("Timed out while extracting %s", a.Path)
	}

	return
}

func fromArchive(path string) (a *archive, err error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return
	}

	a = &archive{
		Path: absolutePath,
	}

	return
}

func buildArchive(destination string, search directory) (a *archive, err error) {
	args := []string{}

	var walker func(scanPath string, info os.FileInfo, err error) error
	walker = func(scanPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			filepath.Walk(info.Name(), walker)
		}

		if !strings.HasSuffix(info.Name(), ".o") {
			return nil
		}

		absolutePath, err := filepath.Abs(scanPath)
		if err != nil {
			return err
		}

		args = append(args, absolutePath)

		return nil
	}

	err = filepath.Walk(search.Path, walker)
	if err != nil {
		return
	}

	args = append([]string{"rcs", destination}, args...)
	cmd := exec.Command("/usr/bin/ar", args...)
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	return
}

func main() {
	flag.Parse()
	temporary, err := newDirectory()
	if err != nil {
		log.Fatalf("Could not provision temporary directory: %s", err)
	}
	defer temporary.Close()
	for _, path := range flag.Args() {
		a, err := fromArchive(path)
		if err != nil {
			log.Fatal(err)
		}
		err = a.Extract(*temporary)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = buildArchive(*outputArchive, *temporary)
	if err != nil {
		log.Fatal(err)
	}
}
