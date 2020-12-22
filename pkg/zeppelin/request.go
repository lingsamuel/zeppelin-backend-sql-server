package zeppelin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

func PostRaw(url string, body interface{}) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	api := fmt.Sprintf("%s/%s", Backend, url)
	logrus.Infof("Params(%v): %v\n", api, string(b))

	resp, err := http.Post(api, "application/json", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func HandleDecodeError(resp *http.Response, err error) error {
	if err != nil {
		c, err2 := ioutil.ReadAll(resp.Body)
		if err2 != nil {
			return errors.New(fmt.Sprintf("decode error: %v. Read body failed: %v", err, err2))
		}
		return errors.New(fmt.Sprintf("decode error: %v. Response body: %v", err, string(c)))
	}
	return nil
}

func Post(url string, body interface{}, r interface{}) error {
	resp, err := PostRaw(url, body)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(resp.Body)

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Error(err.Error())
		}
	}()

	return HandleDecodeError(resp, decoder.Decode(r))
}

func Get(url string) (*json.Decoder, error) {
	api := fmt.Sprintf("%s/%s", Backend, url)
	logrus.Infof("Get(%v)\n", api)

	resp, err := http.Get(api)
	if err != nil {
		return nil, err
	}

	return json.NewDecoder(resp.Body), nil
}

func Delete(url string, r interface{}) error {
	client := &http.Client{}

	api := fmt.Sprintf("%s/%s", Backend, url)
	logrus.Infof("Delete(%v)\n", api)

	req, err := http.NewRequest("DELETE", api, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Error(err.Error())
		}
	}()

	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(r)
	if err != nil {
		return err
	}
	return nil
}
