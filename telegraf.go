package telegraf

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// InvalidUsageError used to describe when an API is used
// incorrectly or in a way that would clash with CLP
type InvalidUsageError struct {
	s string
}

func (e *InvalidUsageError) Error() string {
	return e.s
}

func createDialConn(addr string) (net.Conn, error) {
	URL, err := url.Parse(addr)
	if err != nil {
		return nil, errors.New("Failed to parse addr")
	}

	host := URL.Host
	switch scheme := URL.Scheme; scheme {
	case "tcp":
		return net.Dial("tcp", host)
	case "udp":
		return net.Dial("udp", host)
	case "unix":
		return net.Dial("unix", host)
	default:
		emsg := fmt.Sprintf("Protocol %s not supported", scheme)
		return nil, errors.New(emsg)
	}
}

// ClientImpl ...
type ClientImpl struct {
	conn        net.Conn
	defaultTags map[string]interface{}
}

// NewClientImpl returns an initiated Telegraf client.
//
// Ex usage:
//
// > client := NewClientImpl("tcp://127.0.0.1:8094")
func NewClientImpl(addr string) (*ClientImpl, error) {
	conn, err := createDialConn(addr)
	defaultTags := make(map[string]interface{})

	return &ClientImpl{
		conn,
		defaultTags,
	}, err
}

// AddDefaultTags adds a set of tags which will be added to each metric
func (t *ClientImpl) AddDefaultTags(tags map[string]interface{}) {
	t.defaultTags = tags
}

// Close ...
func (t *ClientImpl) Close() {
	t.conn.Close()
}

// WritePoint will write a single metric. For multiple metrics at once,
// use WritePoints.
func (t *ClientImpl) WritePoint(p *Metric) error {
	if p.Fields == nil {
		return &InvalidUsageError{"metrics must include at least 1 field"}
	}

  p.appendDefaultTags(t.defaultTags)
  
	_, err := fmt.Fprintln(t.conn, p.toLP(true))
	return err
}

// WritePoints writes a slice of metric structs at once.
func (t *ClientImpl) WritePoints(p []*Metric) error {
	var pointArr []string
	for _, m := range p {	
		if m.Fields == nil {
			return &InvalidUsageError{"metrics must include at least 1 field"}
		}
    
    m.appendDefaultTags(t.defaultTags)

		pointArr = append(pointArr, m.toLP(true))
	}
	_, err := fmt.Fprintln(t.conn, strings.Join(pointArr, "\n"))
	return err
}

func (p *Metric) appendDefaultTags(defaultTags map[string]interface{}) {
	for k, v := range defaultTags {
		if p.Tags[k] == nil {
			p.Tags[k] = v
		}
	}
}
