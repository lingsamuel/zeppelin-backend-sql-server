package tester

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kataras/tablewriter"
	"github.com/lensesio/tableprinter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	logLevel int
	db       string
	address  string
	port     int

	user     string
	password string

	testerCmd = &cobra.Command{
		Use:   "zeppelin-proxy-tester",
		Short: "Test a zeppelin backend sql server.",
		Long:  `Test a zeppelin backend sql server.`,
		Run: func(cmd *cobra.Command, args []string) {
			runTester()
		},
	}
)

func Execute(){
	testerCmd.Execute()
}

func init() {
	testerCmd.PersistentFlags().IntVarP(&logLevel, "loglevel", "l", int(logrus.InfoLevel), "Logrus log level. From 0 to 6: panic, fatal, error, warning, info, debug, trace.")
	testerCmd.PersistentFlags().StringVar(&db, "db", "test", "Database name.")

	testerCmd.PersistentFlags().StringVarP(&address, "address", "a", "localhost", "SQL server address.")
	testerCmd.PersistentFlags().IntVarP(&port, "port", "P", 3306, "SQL server port.")

	testerCmd.PersistentFlags().StringVarP(&user, "user", "u", "root", "SQL server user. If user or password empty, auth will be disabled.")
	testerCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "SQL server password. If user or password empty, auth will be disabled.")
}

func runTester() {
	logrus.SetLevel(logrus.Level(logLevel))

	connection := fmt.Sprintf("%s:@tcp(%s:%v)/%s", user, address, port, db)
	fmt.Printf("Connecting: %s\n", connection)

	db, err := sql.Open("mysql", connection)
	if err != nil {
		logrus.Fatalf("can't connect to mysql: %s", err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", ";", -1)

		exec(db, text)
	}
}

func exec(db *sql.DB, text string) {
	rs, err := db.Query(text)
	if err != nil {
		logrus.Fatalf("unable to get rows: %s", err)
	}

	columns, err := rs.Columns()
	if err != nil {
		logrus.Fatalf("unable to get columns: %s", err)
	}
	type Row struct {
		Values []interface{}
	}
	var rows [][]string
	for rs.Next() {
		var row []interface{} = make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			v := ""
			row[i] = &v
		}
		if err := rs.Scan(row...); err != nil {
			logrus.Errorf("got error scanning row: %s", err)
		}

		resultRow := make([]string, len(columns))
		for i := 0; i < len(columns); i++ {
			resultRow[i] = *(row[i].(*string))
		}
		rows = append(rows, resultRow)
	}

	printer := tableprinter.New(os.Stdout)

	printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
	printer.CenterSeparator = "│"
	printer.ColumnSeparator = "│"
	printer.RowSeparator = "─"
	printer.HeaderFgColor = tablewriter.FgGreenColor
	printer.Render(columns, rows, []int{}, true)
}
