package zeppelin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	Backend string
)

type Client struct {
	sessionId string
	noteId    string
}

func New() (*Client, error) {
	c := &Client{}

	var createSessionResp struct {
		Status string `json:"status"`
		Body   struct {
			SessionId string `json:"sessionId"`
			NoteId    string `json:"noteId"`
		} `json:"body"`
	}
	err := Post("session", struct {
		Interpreter string `json:"interpreter"`
	}{
		Interpreter: "flink",
	}, &createSessionResp)
	if err != nil {
		return nil, err
	}
	if createSessionResp.Status != "OK" {
		return nil, errors.New(fmt.Sprintf("%s", createSessionResp.Status))
	}

	c.sessionId = createSessionResp.Body.SessionId
	c.noteId = createSessionResp.Body.NoteId

	return c, nil
}

func (c *Client) RunParagraph(para string) ([]*sqltypes.Result, error) {
	para = "%flink.bsql\n" + para

	var createResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Body    string `json:"body"`
	}
	err := Post(fmt.Sprintf("notebook/%s/paragraph", c.noteId), struct {
		Title string `json:"title"`
		Text  string `json:"text"`
	}{
		Text: para,
	}, &createResp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("create paragraph: %v", err))
	}

	paragraphId := createResp.Body

	resp, err := PostRaw(fmt.Sprintf("notebook/run/%s/%s", c.noteId, paragraphId), struct{}{})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("run paragraph: %v", err))
	}
	decoder := json.NewDecoder(resp.Body)

	var body json.RawMessage
	var runResp = struct {
		Status string      `json:"status"`
		Body   interface{} `json:"body"`
	}{
		Body: &body,
	}
	err = decoder.Decode(&runResp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("decode run paragraph result: %v", HandleDecodeError(resp, err)))
	}
	var msgBody struct {
		Code string `json:"code"`
		Msg  []struct {
			Type string `json:"type"`
			Data string `json:"data"`
		} `json:"msg"`
	}
	switch runResp.Status {
	case "OK":
		err = json.Unmarshal(body, &msgBody)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("decode success run paragraph result: %v", HandleDecodeError(resp, err)))
		}
	default:
		var errBody struct {
			Code string `json:"code"`
			Type string `json:"type"`
			Msg  string `json:"msg"`
		}
		err = json.Unmarshal(body, &errBody)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("decode fail run paragraph result: %v", HandleDecodeError(resp, err)))
		}
		return nil, errors.New(fmt.Sprintf("%s: %s", errBody.Code, errBody.Msg))
	}

	var results []*sqltypes.Result
	for _, msg := range msgBody.Msg {
		result, err := parseMsg(msg.Data)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("parse msg %s: %v", msg, err))
		}
		results = append(results, result)
	}
	return results, nil
}

func parseMsg(msg string) (*sqltypes.Result, error) {
	rows := strings.Split(msg, "\n")
	if len(rows) == 0 {
		return nil, errors.New(fmt.Sprintf("Msg parse error: %v\n", msg))
	}
	fields := strings.Split(rows[0], "\t")
	rows = rows[1:]

	var pbFields []*querypb.Field
	for _, field := range fields {
		pbFields = append(pbFields,
			&querypb.Field{
				Name:     field,
				Type:     querypb.Type_TEXT,
				Table:    "test",
				OrgTable: "test",
				Database: "test",
				OrgName:  "test",
			})
	}

	pbRows := [][]sqltypes.Value{}
	for _, row := range rows {
		if row == "" {
			continue
		}

		pbRow := []sqltypes.Value{}
		values := strings.Split(row, "\t")
		for _, value := range values {
			v, err := sqltypes.NewValue(querypb.Type_TEXT, []byte(value))
			if err != nil {
				return nil, err
			}
			pbRow = append(pbRow, v)

		}
		pbRows = append(pbRows, pbRow)
	}

	return &sqltypes.Result{
		Fields:       pbFields,
		RowsAffected: 0,
		InsertID:     0,
		Rows:         pbRows,
	}, nil
}

func (c *Client) Disconnect() error {
	var err error
	defer func() {
		if err != nil {
			logrus.Errorf("Disconnect: %v", err)
		}
	}()

	var deleteResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	err = Delete(fmt.Sprintf("session/%s", c.sessionId), &deleteResp)
	if err != nil {
		return err
	}
	if deleteResp.Status != "OK" {
		return errors.New(fmt.Sprintf("%s: %s", deleteResp.Status, deleteResp.Message))
	}

	return nil
}
