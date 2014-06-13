package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const _Suffix = ".tar.gz."

type backupEntry struct {
	Name string
	Path string
}

type backupConfig struct {
	Dst     string
	KeepGen int
	Entries []*backupEntry
}

func (config *backupConfig) isValid() error {
	if err := config.isDstWritable(); err != nil {
		return err
	}

	if err := config.isNameDuplicated(); err != nil {
		return err
	}

	return nil
}

func (config *backupConfig) isDstWritable() error {
	fi, err := os.Stat(config.Dst)
	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("config.Dst is not directory. dir=%s", config.Dst)
	}

	return nil
}

func (config *backupConfig) isNameDuplicated() error {
	m := map[string]struct{}{}

	for _, e := range config.Entries {
		_, exists := m[e.Name]
		if exists {
			return fmt.Errorf("duplicated name found in config.Entries. name=%s", e.Name)
		}
		m[e.Name] = struct{}{}
	}

	return nil
}

func readConfig() (*backupConfig, error) {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to json config file")
	flag.Parse()

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &backupConfig{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return config, nil
}

type result struct {
	name string
	err  error
}
type resultCh chan result

type tsSortable []string

func (a tsSortable) Len() int      { return len(a) }
func (a tsSortable) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a tsSortable) Less(i, j int) bool {
	var tsi, tsj int64
	var err error
	if tsi, err = strconv.ParseInt(filepath.Ext(a[i])[1:], 10, 64); err != nil {
		panic(err.Error())
	}
	if tsj, err = strconv.ParseInt(filepath.Ext(a[j])[1:], 10, 64); err != nil {
		panic(err.Error())
	}
	return tsi < tsj
}

func backupImpl(ch resultCh, wg *sync.WaitGroup, i int, config *backupConfig) {
	defer wg.Done()
	ent := config.Entries[i]

	// do backup
	now := time.Now().Unix()
	tgz := filepath.Join(config.Dst, fmt.Sprintf("%s%s%d", ent.Name, _Suffix, now))
	cmd := exec.Command("tar", "zcf", tgz, ent.Path)
	if err := cmd.Run(); err != nil {
		ch <- result{ent.Name, err}
		return
	}
	// delete old backups
	matchs, err := filepath.Glob(filepath.Join(config.Dst, ent.Name+_Suffix+"*"))
	if err != nil {
		ch <- result{ent.Name, err}
	}
	sort.Sort(tsSortable(matchs))
	for len(matchs) > config.KeepGen {
		if err := os.Remove(matchs[0]); err != nil {
			ch <- result{ent.Name, err}
		}
		matchs = matchs[1:]
	}

	ch <- result{ent.Name, nil}
}

func backup(config *backupConfig) {
	wg := &sync.WaitGroup{}
	rch := make(resultCh)

	for i, _ := range config.Entries {
		wg.Add(1)
		go backupImpl(rch, wg, i, config)
	}

	go func() {
		wg.Wait()
		close(rch)
	}()

	for r := range rch {
		fmt.Printf("%s %#v\n", r.name, r.err)
	}
	wg.Wait()
}

func main() {
	config, err := readConfig()
	if err != nil {
		log.Fatalln(err)
	}

	if err := config.isValid(); err != nil {
		log.Fatalln(err)
	}

	backup(config)
}
