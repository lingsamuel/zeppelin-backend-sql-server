package cmd

import (
	"fmt"
	"os"

	"github.com/lingsamuel/zeppelin-backend-sql-server/pkg/server"
	"github.com/lingsamuel/zeppelin-backend-sql-server/pkg/zeppelin"
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

	zeppelinAddress string
	zeppelinPort    string

	RootCmd = &cobra.Command{
		Use:   "zeppelin-proxy",
		Short: "Provides a zeppelin backend sql server.",
		Long:  `Provides a zeppelin backend sql server.`,
		Run: func(cmd *cobra.Command, args []string) {
			runRoot()
		},
	}
)

func init() {
	RootCmd.PersistentFlags().IntVarP(&logLevel, "loglevel", "l", int(logrus.InfoLevel), "Logrus log level. From 0 to 6: panic, fatal, error, warning, info, debug, trace.")
	RootCmd.PersistentFlags().StringVar(&db, "db", "test", "Database name.")

	RootCmd.PersistentFlags().StringVarP(&address, "address", "a", "0.0.0.0", "SQL server address.")
	RootCmd.PersistentFlags().IntVarP(&port, "port", "P", 3306, "SQL server port.")

	RootCmd.PersistentFlags().StringVarP(&user, "user", "u", "", "SQL server user. If user or password empty, auth will be disabled.")
	RootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "SQL server password. If user or password empty, auth will be disabled.")

	RootCmd.PersistentFlags().StringVar(&zeppelinAddress, "zeppelinAddress", "", "Zeppelin address.")
	RootCmd.PersistentFlags().StringVar(&zeppelinPort, "zeppelinPort", "80", "Zeppelin port.")
}

func check() {
	if zeppelinAddress == "" {
		logrus.Fatal("zeppelinAddress can't be empty.")
		os.Exit(1)
	}
	if zeppelinPort == "" {
		logrus.Fatal("zeppelinPort can't be empty.")
		os.Exit(1)
	}
}

func runRoot() {
	logrus.SetLevel(logrus.Level(logLevel))
	check()

	zeppelin.Backend = fmt.Sprintf("%s:%s/api", zeppelinAddress, zeppelinPort)

	s, err := server.New("tcp", fmt.Sprintf("%s:%v", address, port))
	if err != nil {
		panic(err)
	}

	s.ServerVersion = "1.0.0-Zeppelin"
	logrus.Infof("Started at %s (backend %s)", s.Addr(), zeppelin.Backend)
	s.Accept()
}
