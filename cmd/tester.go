package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const connectionString = "root:@tcp(127.0.0.1:3306)/mydb"

func main() {
	fmt.Printf("Connecting: %s\n", connectionString)

	db, err := sql.Open("mysql", connectionString)
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

	fmt.Printf("Result: %v\n", rs)
}
