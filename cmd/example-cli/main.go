//nolint:all
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/w1ck3dg0ph3r/dbadger"
)

func main() {
	app := newApplication()
	app.handler = func(cmd string) {
		printUsage := func() {
			buf := &bytes.Buffer{}
			buf.WriteString("\nSupported commands:\n")
			buf.WriteString("\tset {key} {value} // Set a key-value pair\n")
			buf.WriteString("\tdel {key} // Remove a key\n")
			buf.WriteString("\tsetmany // Set 1000 test key-value pairs\n")
			buf.WriteString("\tdrop // Remove all keys\n")
			buf.WriteString("\tsnapshot // Take a snapshot\n")
			buf.WriteString("\trestore {snapshot_id} // Restore from a snapshot\n")
			_, _ = app.logBox.Write(buf.Bytes())
		}
		parts := strings.Split(cmd, " ")
		switch {
		case parts[0] == "set":
			if len(parts) != 3 {
				printUsage()
				break
			}
			if err := app.db.SetString(context.Background(), parts[1], parts[2]); err != nil {
				app.logger.Errorf("set: %v", err)
			}
		case parts[0] == "del":
			if len(parts) != 2 {
				printUsage()
				break
			}
			if err := app.db.DeleteString(context.Background(), parts[1]); err != nil {
				app.logger.Errorf("del: %v", err)
			}
		case parts[0] == "testset":
			var keys [][]byte
			var values [][]byte
			for i := 0; i < 1000; i++ {
				keys = append(keys, []byte(fmt.Sprintf("testkey_%06d", i+1)))
				values = append(values, []byte(fmt.Sprintf("testvalue_%06d", i+1)))
			}
			if err := app.db.SetMany(context.Background(), keys, values); err != nil {
				app.logger.Errorf("setmany: %v", err)
			}
		case parts[0] == "drop":
			if err := app.db.DeleteAll(context.Background()); err != nil {
				app.logger.Errorf("clear: %v", err)
			}
		case parts[0] == "snapshot":
			id, err := app.db.Snapshot()
			if err != nil {
				app.logger.Errorf("backup: %v", err)
				break
			}
			app.logger.Infof("taken snapshot: %s", id)
		case parts[0] == "restore":
			if len(parts) != 2 {
				printUsage()
				break
			}
			if err := app.db.Restore(parts[1]); err != nil {
				app.logger.Errorf("restore: %v", err)
			}
		default:
			printUsage()
		}
	}
	app.run()
}

type Application struct {
	app *tview.Application
	db  *dbadger.DB

	dataBox      *tview.Table
	nodesBox     *tview.List
	nodeStatsBox *tview.TextView
	logBox       *tview.TextView
	cmdBox       *tview.InputField

	logger  dbadger.Logger
	handler func(cmd string)

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func newApplication() *Application {
	app := &Application{
		app:        tview.NewApplication(),
		shutdownCh: make(chan struct{}),
	}
	app.dataBox = tview.NewTable().SetBorders(true).SetFixed(1, 0)
	app.dataBox.SetBorder(true).SetTitle("Data")

	app.nodesBox = tview.NewList().ShowSecondaryText(false)
	app.nodesBox.SetBorder(true).SetTitle("Nodes")

	app.nodeStatsBox = tview.NewTextView()
	app.nodeStatsBox.SetBorder(true).SetTitle("Node Stats")
	app.nodeStatsBox.SetChangedFunc(app.draw)

	app.logBox = tview.NewTextView()
	app.logBox.SetBorder(true).SetTitle("Logs")
	app.logBox.SetScrollable(false)
	app.logBox.SetChangedFunc(app.draw)
	app.logger = &textViewLogger{textView: app.logBox}

	app.cmdBox = tview.NewInputField().SetLabel("$> ").SetFieldStyle(tcell.Style{})
	app.cmdBox.SetBorder(true)
	app.cmdBox.SetDoneFunc(func(key tcell.Key) {
		cmd := app.cmdBox.GetText()
		app.cmdBox.SetText("")
		app.handler(cmd)
	})

	body := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(
			tview.NewFlex().SetDirection(tview.FlexColumn).
				AddItem(app.dataBox, 0, 1, false).
				AddItem(
					tview.NewFlex().SetDirection(tview.FlexRow).
						AddItem(app.nodesBox, 0, 1, false).
						AddItem(app.nodeStatsBox, 0, 1, false),
					0, 1, false).
				AddItem(app.logBox, 0, 2, false),
			0, 1, false,
		).
		AddItem(app.cmdBox, 3, 0, true)

	app.app.SetRoot(body, true)

	app.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlC {
			go app.quit()
			return nil
		}
		return event
	})

	return app
}

func (app *Application) run() {
	flags := parseFlags()

	config := &dbadger.Config{
		Path:     flags.Path,
		InMemory: flags.Inmem,
		Bind:     dbadger.Address(flags.Bind),
		TLS: dbadger.TLSConfig{
			CAFile:   flags.TLS.CAFile,
			CertFile: flags.TLS.CertFile,
			KeyFile:  flags.TLS.KeyFile,
		},
		Bootstrap: flags.Bootstrap,
		Recover:   flags.Recover,
		Join:      dbadger.Address(flags.Join),
		Logger:    app.logger,
	}

	if flags.Bootstrap && !flags.Inmem && filepath.IsLocal(flags.Path) {
		_ = os.RemoveAll(flags.Path)
	}

	go func() {
		var err error
		app.db, err = dbadger.Start(config)
		if err != nil {
			app.logger.Errorf(err.Error())
			return
		}
		app.wg.Add(1)
		go app.refreshData()
		app.wg.Add(1)
		go app.refreshNodes()
	}()

	if err := app.app.Run(); err != nil {
		panic(err)
	}
}

func (app *Application) quit() {
	if app.shutdownCh == nil {
		return
	}
	app.logger.Infof("quitting...")
	close(app.shutdownCh)
	app.wg.Wait()
	app.shutdownCh = nil
	if app.db != nil {
		if err := app.db.Stop(); err != nil {
			app.logger.Errorf(err.Error())
		}
		time.Sleep(1500 * time.Millisecond)
	}
	app.app.Stop()
}

func (app *Application) draw() {
	if app.app == nil {
		return
	}
	app.app.Draw()
}

func (app *Application) setDataHeader() {
	app.dataBox.SetCell(0, 0, &tview.TableCell{Text: "Key", Expansion: 1, Align: tview.AlignCenter})
	app.dataBox.SetCell(0, 1, &tview.TableCell{Text: "Value", Expansion: 1, Align: tview.AlignCenter})
}

func (app *Application) refreshData() {
	defer app.wg.Done()
	for {
		select {
		case <-app.shutdownCh:
			return
		case <-time.Tick(250 * time.Millisecond):
			keys, values, err := app.db.GetRange(context.Background(), nil, nil, 100, dbadger.LocalPreference)
			if err != nil {
				app.logger.Errorf("get range: %v", err)
			}
			app.dataBox.Clear()
			app.setDataHeader()
			for i := range keys {
				app.dataBox.SetCell(i+1, 0, &tview.TableCell{Text: string(keys[i]), Expansion: 1})
				app.dataBox.SetCell(i+1, 1, &tview.TableCell{Text: string(values[i]), Expansion: 1})
			}
			app.dataBox.ScrollToBeginning()
			app.draw()
		}
	}
}

func (app *Application) refreshNodes() {
	defer app.wg.Done()
	for {
		select {
		case <-app.shutdownCh:
			return
		case <-time.Tick(500 * time.Millisecond):
			app.nodesBox.Clear()
			leader := app.db.Leader()
			for i, node := range app.db.Nodes() {
				title := strings.Builder{}
				title.WriteString(string(node))
				if node == leader {
					title.WriteString(` (LEADER)`)
				}
				app.nodesBox.AddItem(title.String(), "", 0, nil)
				if node == app.db.Addr() {
					app.nodesBox.SetCurrentItem(i)
				}
			}
			stats := app.db.Stats()
			statsKeys := make([]string, 0, len(stats))
			for k := range stats {
				statsKeys = append(statsKeys, k)
			}
			sort.Strings(statsKeys)
			statsText := &bytes.Buffer{}
			for _, k := range statsKeys {
				statsText.WriteString(fmt.Sprintf("%s: %s\n", k, stats[k]))
			}
			app.nodeStatsBox.SetText(statsText.String())
			app.draw()
		}
	}
}

type Flags struct {
	Path      string
	Inmem     bool
	Bind      string
	Bootstrap bool
	Recover   bool
	Join      string

	TLS struct {
		CAFile   string
		CertFile string
		KeyFile  string
	}
}

func parseFlags() *Flags {
	var flags Flags
	set := flag.NewFlagSet("dbadger", flag.ExitOnError)
	set.StringVar(&flags.Path, "db", "", "Database path")
	set.BoolVar(&flags.Inmem, "inmem", false, "In memory mode")
	set.StringVar(&flags.Bind, "bind", "127.0.0.1:7421", "Bind address")
	set.BoolVar(&flags.Bootstrap, "bootstrap", false, "Bootstrap cluster")
	set.BoolVar(&flags.Recover, "recover", false, "Recover cluster")
	set.StringVar(&flags.Join, "join", "", "Join cluster")

	set.StringVar(&flags.TLS.CAFile, "ca", "", "TLS CA certificate filename")
	set.StringVar(&flags.TLS.CertFile, "cert", "", "TLS certificate filename")
	set.StringVar(&flags.TLS.KeyFile, "key", "", "TLS key filename")

	_ = set.Parse(os.Args[1:])

	if err := flags.Validate(); err != nil {
		fmt.Println(err)
		fmt.Println()
		set.Usage()
		os.Exit(1)
	}

	return &flags
}

func (f *Flags) Validate() error {
	var err error
	if f.Path == "" && !f.Inmem {
		return fmt.Errorf("either db or inmem must be set")
	}
	if f.Bind == "" {
		return fmt.Errorf("bind address must be set")
	}
	_, err = net.ResolveTCPAddr("tcp", f.Bind)
	if err != nil {
		return fmt.Errorf("invalid bind address: %w", err)
	}
	if f.Bootstrap && f.Recover {
		return fmt.Errorf("can not use bootstrap and recover together")
	}
	if f.Bootstrap && f.Join != "" {
		return fmt.Errorf("can not use bootstrap and join together")
	}
	if f.Recover && f.Join != "" {
		return fmt.Errorf("can not use recover and join together")
	}
	if f.Join != "" {
		_, err = net.ResolveTCPAddr("tcp", f.Join)
		if err != nil {
			return fmt.Errorf("invalid join address: %w", err)
		}
	}
	return nil
}

type textViewLogger struct {
	textView *tview.TextView
}

func (l *textViewLogger) printf(level, format string, args ...any) {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("[%s] ", level))
	buf.WriteString(fmt.Sprintf(format, args...))
	buf.WriteRune('\n')
	_, _ = l.textView.Write(buf.Bytes())
}

func (l *textViewLogger) Debugf(format string, args ...any)   { l.printf("DEBU", format, args...) }
func (l *textViewLogger) Infof(format string, args ...any)    { l.printf("INFO", format, args...) }
func (l *textViewLogger) Warningf(format string, args ...any) { l.printf("WARN", format, args...) }
func (l *textViewLogger) Errorf(format string, args ...any)   { l.printf("ERRO", format, args...) }
func (l *textViewLogger) Fatalf(format string, args ...any)   { l.printf("FATA", format, args...) }
