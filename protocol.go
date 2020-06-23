package telegraf

import (
	"fmt"
	"strings"
	"time"
)

// Metric ...
type Metric struct {
	Measurement string
	Tags        map[string]interface{}
	Fields      map[string]interface{}
}

// toLP translates the initiated metric struct to the outgoing
// influxDB line protocol format
func (t *Metric) toLP(withTimestamp bool) string {
	tags := formatTags(t.Tags)
	fields := formatAttr(t.Fields, true)

	base := fmt.Sprintf("%s%s %s", t.Measurement, tags, fields)

	if withTimestamp {
		utc, _ := time.LoadLocation("UTC")
		utcNow := time.Now().In(utc)
		unixNow := utcNow.UnixNano()
		base += fmt.Sprintf(" %d", unixNow)
	}

	return base
}

func formatTags(tags map[string]interface{}) string {
	if tags == nil {
		return ""
	}

	return fmt.Sprintf(",%s", formatAttr(tags, false))
}

func escapeString(s string) string {
	return strings.NewReplacer(`,`, `\,`, ` `, `\ `, `=`, `\=`).Replace(s)
}

func getFieldString(value interface{}) string {
	switch value.(type) {
	case int:
		return fmt.Sprintf(`%vi`, value)
	case string:
		return fmt.Sprintf(`"%s"`, value)
	default:
		return fmt.Sprintf(`%v`, value)
	}
}

// formatAttr formats a single metric attribute to LP format
func formatAttr(attr map[string]interface{}, field bool) string {
	var values []string
	for key, value := range attr {
		var indValue string
		if field {
			indValue = getFieldString(value)
		} else {
			// Tags are always strings, and then must be escaped
			indValue = escapeString(fmt.Sprintf(`%v`, value))
		}

		values = append(values, fmt.Sprintf("%v=%v", key, indValue))
	}
	return strings.Join(values, ",")
}
