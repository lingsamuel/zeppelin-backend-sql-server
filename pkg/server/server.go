package server

import (
	"fmt"
	"github.com/lingsamuel/zeppelin-backend-sql-server/pkg/zeppelin"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func New(network, address string) (*mysql.Listener, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return mysql.NewFromListener(l, &mysql.AuthServerNone{}, NewHandler(0), 0, 0)
}

type conntainer struct {
	MysqlConn *mysql.Conn
	NetConn   net.Conn
}

type Handler struct {
	mu          sync.Mutex
	c           map[uint32]conntainer
	readTimeout time.Duration
	lc          []*net.Conn

	client *zeppelin.Client
}

var _ mysql.Handler = (*Handler)(nil)

func NewHandler(rt time.Duration) *Handler {
	return &Handler{
		c:           make(map[uint32]conntainer),
		readTimeout: rt,
	}
}

// AddNetConnection is used to add the net.Conn to the Handler when available (usually on the
// Listener.Accept() method)
func (h *Handler) AddNetConnection(c *net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lc = append(h.lc, c)
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	h.mu.Lock()
	if _, ok := h.c[c.ConnectionID]; !ok {
		// Retrieve the latest net.Conn stored by Listener.Accept(), if called, and remove it
		var netConn net.Conn
		if len(h.lc) > 0 {
			netConn = *h.lc[len(h.lc)-1]
			h.lc = h.lc[:len(h.lc)-1]
		} else {
			logrus.Debug("Could not find TCP socket connection after Accept(), " +
				"connection checker won't run")
		}
		h.c[c.ConnectionID] = conntainer{c, netConn}
	}

	h.mu.Unlock()

	logrus.Infof("NewConnection: client %v", c.ConnectionID)

	var err error
	h.client, err = zeppelin.New()
	if err != nil {
		panic(err)
	}
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
	fmt.Printf("ComInitDB %s\n", schemaName)
	return nil
}

func (h *Handler) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	fmt.Printf("ComPrepare %s\n", query)
	return []*querypb.Field{}, nil
}

func (h *Handler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	//return h.doQuery(c, prepare.PrepareStmt, prepare.BindVars, callback)
	fmt.Printf("ComStmtExecute %v\n", prepare)
	return nil
}

func (h *Handler) ComResetConnection(c *mysql.Conn) {
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	h.mu.Lock()
	delete(h.c, c.ConnectionID)
	h.mu.Unlock()

	logrus.Infof("ConnectionClosed: client %v", c.ConnectionID)
	err := h.client.Disconnect()
	if err != nil {
		panic(err)
	}
}

// ComQuery executes a SQL query
func (h *Handler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	var err error
	defer func() {
		if err != nil {
			logrus.Errorf("ComQuery: %v", err)
		}
	}()

	if strings.Contains(query, "@@version_comment") {
		var v sqltypes.Value
		v, err = sqltypes.NewValue(querypb.Type_TEXT, []byte(fmt.Sprintf("Configured at %s", zeppelin.Backend)))
		if err != nil {
			return err
		}
		err = callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				&querypb.Field{
					Name:     "@@version_comment",
					Type:     querypb.Type_TEXT,
					Table:    "test",
					OrgTable: "test",
					Database: "test",
					OrgName:  "test",
				},
			},
			RowsAffected: 0,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					v,
				},
			},
		})
		return err
	}

	fmt.Printf("ComQuery %s\n", query)
	r, err := h.client.RunParagraph(query)
	if err != nil {
		return err
	}

	// return last query
	if len(r) == 0 {
		return nil
	}
	err = callback(r[len(r)-1])
	if err != nil {
		return err
	}
	return nil
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery callback if the result does not contain any fields,
// or after the last ComQuery call completes.
func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}
