package pontoon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

// A custom http.Transport with support for deadline timeouts
func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

func apiRequest(method string, endpoint string, body interface{}, timeout time.Duration) (*simplejson.Json, error) {
	httpclient := &http.Client{Transport: newDeadlineTransport(timeout)}

	js, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(js)

	req, err := http.NewRequest(method, endpoint, buf)
	if err != nil {
		return nil, err
	}

	resp, err := httpclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	rb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data, err := simplejson.NewJson(rb)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("response status_code = %d", resp.StatusCode))
	}

	return data, err
}
