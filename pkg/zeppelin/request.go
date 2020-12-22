package zeppelin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"
)

func Post(url string, body interface{}) (*json.Decoder, error) {

	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Params: %v\n", string(b))

	resp, err := http.Post(fmt.Sprintf("%s/%s", Backend, url), "application/json", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	return json.NewDecoder(resp.Body), nil
}

func Get(url string) (*json.Decoder, error) {
	resp, err := http.Get(fmt.Sprintf("%s/%s", Backend, url))
	if err != nil {
		return nil, err
	}

	return json.NewDecoder(resp.Body), nil
}

func Delete(url string) (*json.Decoder, error) {
	client := &http.Client{}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s", Backend, url), nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Error(err.Error())
		}
	}()

	return json.NewDecoder(resp.Body), nil
}
