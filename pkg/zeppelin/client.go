package zeppelin

import (
	"errors"
	"fmt"
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
	return c,nil
	r, err := Post("session", struct {
		Interpreter string `json:"interpreter"`
	}{
		Interpreter: "flink",
	})
	if err != nil {
		return nil, err
	}
	var createSessionResp struct {
		SessionId string `json:"sessionId"`
		NoteId    string `json:"noteId"`
	}
	err = r.Decode(&createSessionResp)
	if err != nil {
		return nil, err
	}
	c.sessionId = createSessionResp.SessionId
	c.noteId = createSessionResp.NoteId

	return c, nil
}

type Result struct {
}

func (c *Client) RunParagraph(para string) ([]Result, error) {
	para = "%flink.bsql\n" + para

	resp, err := Post(fmt.Sprintf("notebook/%s/paragraph", c.noteId), struct {
		Title string `json:"title"`
		Text  string `json:"text"`
	}{
		Text: para,
	})
	if err != nil {
		return nil, err
	}

	var createResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Body    string `json:"body"`
	}
	err = resp.Decode(&createResp)
	if err != nil {
		return nil, err
	}
	paragraphId := createResp.Body

	resp, err = Post(fmt.Sprintf("run/%s/%s", c.noteId, paragraphId), struct{}{})
	if err != nil {
		return nil, err
	}

	var runResp struct {
		Status string `json:"status"`
	}
	err = resp.Decode(&runResp)
	if err != nil {
		return nil, err
	}
	if runResp.Status != "OK" {
		var failResp struct {
			Status string `json:"status"`
			Body   struct {
				Code string `json:"code"`
				Type string `json:"type"`
				Msg  string `json:"msg"`
			} `json:"body"`
		}
		err = resp.Decode(&failResp)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(fmt.Sprintf("%s: %s", failResp.Body.Code, failResp.Body.Msg))
	}

	var successResp struct {
		Status string `json:"status"`
		Body   struct {
			Code string `json:"code"`
			Msg  []struct {
				Type string `json:"type"`
				Data string `json:"data"`
			} `json:"msg"`
		} `json:"body"`
	}
	err = resp.Decode(&successResp)
	if err != nil {
		return nil, err
	}
	var result []Result

	return result, nil
}

func (c *Client) Disconnect() error {
	return nil
	resp, err := Delete(fmt.Sprintf("session/%s", c.sessionId))
	if err != nil {
		return err
	}
	var deleteResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	err = resp.Decode(&deleteResp)
	if err != nil {
		return err
	}
	if deleteResp.Status != "OK" {
		return errors.New(fmt.Sprintf("%s: %s", deleteResp.Status, deleteResp.Message))
	}

	return nil
}
