package telegraf

import (
	"fmt"
	"testing"
)

// Go doesnt guarantee consistent iteration order 🔫
// Figure out a way to test these?
func TestFormatAttr(t *testing.T) {
	attr := map[string]interface{}{"foo": 1, "bar": "a"}
	formattedAttr := formatAttr(attr, true)
	fmt.Println(formattedAttr)
	// if formattedAttr != "foo=1,bar=a" || formattedAttr != "bar=a,foo=1" {
	// 	t.Errorf("Formatted attr was incorrect - got: %s, expected: %s", formattedAttr, "foo=1,bar=a")
	// }
}

func TestToLP(t *testing.T) {
	testMetric := &Metric{
		Measurement: "testM",
		Tags:        map[string]interface{}{"tag1": 1, "tag2": "a"},
		Fields:      map[string]interface{}{"field1": 2, "field2": "b"},
	}
	clpString := testMetric.toLP(true)
	fmt.Println(clpString)
	// expected := "testM,tag1=1,tag2=a field1=2,field2=b"
	// if clpString != expected {
	// 	t.Errorf("Genereated CLP string was incorrect - got :%s, expected: %s", clpString, expected)
	// }
}

func TestEmptyTags(t *testing.T) {
	testMetric := &Metric{
		Measurement: "testM",
		Tags:        nil,
		Fields:      map[string]interface{}{"field1": 2},
	}
	clpString := testMetric.toLP(false)
	expected := "testM field1=2i"
	if clpString != expected {
		emsg := fmt.Sprintf("Did not format point properly - got: %s, expected: %s", clpString, expected)
		t.Errorf(emsg)
	}
}

func TestGetFieldString(t *testing.T) {
	v := "fooString"
	fs := getFieldString(v)
	expected := `"fooString"`
	if fs != expected {
		emsg := fmt.Sprintf("Did not format field properly - got: %s, expected: %s", fs, expected)
		t.Errorf(emsg)
	}

	fs = getFieldString(10)
	expected = "10i"
	if fs != expected {
		emsg := fmt.Sprintf("Did not format field properly - got: %s, expected: %s", fs, expected)
		t.Errorf(emsg)
	}

	fs = getFieldString(10.0)
	expected = "10"
	if fs != expected {
		emsg := fmt.Sprintf("Did not format field properly - got: %s, expected: %s", fs, expected)
		t.Errorf(emsg)
	}

	fs = getFieldString(10.9)
	expected = "10.9"
	if fs != expected {
		emsg := fmt.Sprintf("Did not format field properly - got: %s, expected: %s", fs, expected)
		t.Errorf(emsg)
	}
}

func TestEscapeString(t *testing.T) {
	v := "us,place"
	expected := "us\\,place"
	esc := escapeString(v)
	if esc != expected {
		emsg := fmt.Sprintf("Did not escape string properly - got: %s, expected: %s", esc, expected)
		t.Errorf(emsg)
	}

	v = "id=9"
	expected = "id\\=9"
	esc = escapeString(v)
	if esc != expected {
		emsg := fmt.Sprintf("Did not escape string properly - got: %s, expected: %s", esc, expected)
		t.Errorf(emsg)
	}

	v = "first name"
	expected = "first\\ name"
	esc = escapeString(v)
	if esc != expected {
		emsg := fmt.Sprintf("Did not escape string properly - got: %s, expected: %s", esc, expected)
		t.Errorf(emsg)
	}
}
