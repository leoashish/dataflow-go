package ingest

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// sink applies the ErrorPolicy for both readers.
type sink struct {
	policy ErrorPolicy
	bad    []BadRecord
}

// reject handles a bad record; a non-nil return means "stop now with this error".
func (s *sink) reject(line int, raw string, err error) error {
	err = fmt.Errorf("%w: %v", ErrBadRecord, err)
	switch s.policy {
	case FailFast:
		s.bad = append(s.bad, BadRecord{Line: line, Raw: raw, Err: err})
		return fmt.Errorf("line %d: %w", line, err)
	case Collect:
		s.bad = append(s.bad, BadRecord{Line: line, Raw: raw, Err: err})
	}
	return nil // Skip
}

func ReadCSV(r io.Reader, schema Schema, policy ErrorPolicy, yield func(Row) error) ([]BadRecord, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // ragged records are allowed
	s := &sink{policy: policy}

	header, err := cr.Read()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the header: %w", err)
	}
	index := map[string]int{}
	for i, name := range header {
		if _, dup := index[name]; !dup {
			index[name] = i
		}
	}
	for _, f := range schema {
		if _, ok := index[f.Name]; !ok && f.Required {
			return nil, fmt.Errorf("%w: %q", ErrMissingColumn, f.Name)
		}
	}

	for {
		record, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return s.bad, nil
		}
		if err != nil {
			var pe *csv.ParseError
			line := 0
			if errors.As(err, &pe) {
				line = pe.StartLine
			}
			if stop := s.reject(line, "", err); stop != nil {
				return s.bad, stop
			}
			continue
		}
		line, _ := cr.FieldPos(0)
		row, rerr := csvRow(record, index, schema)
		if rerr != nil {
			if stop := s.reject(line, strings.Join(record, ","), rerr); stop != nil {
				return s.bad, stop
			}
			continue
		}
		if err := yield(row); err != nil {
			return s.bad, err
		}
	}
}

func csvRow(record []string, index map[string]int, schema Schema) (Row, error) {
	row := make(Row, len(schema))
	for _, f := range schema {
		raw := ""
		if i, ok := index[f.Name]; ok && i < len(record) {
			raw = strings.TrimSpace(record[i])
		}
		if raw == "" {
			if f.Required {
				return nil, fmt.Errorf("field %q is required", f.Name)
			}
			row[f.Name] = nil
			continue
		}
		v, err := Coerce(raw, f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %v", f.Name, err)
		}
		row[f.Name] = v
	}
	return row, nil
}

func ReadNDJSON(r io.Reader, schema Schema, policy ErrorPolicy, yield func(Row) error) ([]BadRecord, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	s := &sink{policy: policy}
	for line := 1; ; line++ {
		raw, err := br.ReadBytes('\n') // allocates per line, but handles lines of any length
		if len(raw) > 0 {
			text := string(bytes.TrimRight(raw, "\r\n"))
			if strings.TrimSpace(text) != "" {
				row, rerr := jsonRow(text, schema)
				if rerr != nil {
					if stop := s.reject(line, text, rerr); stop != nil {
						return s.bad, stop
					}
				} else if yerr := yield(row); yerr != nil {
					return s.bad, yerr
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return s.bad, nil
		}
		if err != nil {
			return s.bad, err
		}
	}
}

func jsonRow(text string, schema Schema) (Row, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber() // keep numbers as text: float64 would corrupt integers above 2^53
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("invalid JSON object: %v", err)
	}
	row := make(Row, len(schema))
	for _, f := range schema {
		v, present := obj[f.Name]
		if !present || v == nil {
			if f.Required {
				return nil, fmt.Errorf("field %q is required", f.Name)
			}
			row[f.Name] = nil
			continue
		}
		typed, err := fromJSON(v, f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %v", f.Name, err)
		}
		row[f.Name] = typed
	}
	return row, nil
}

func fromJSON(v any, t FieldType) (any, error) {
	switch t {
	case String:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case Int:
		if n, ok := v.(json.Number); ok {
			i, err := n.Int64()
			if err != nil {
				return nil, fmt.Errorf("not an integer: %s", n)
			}
			return i, nil
		}
	case Float:
		if n, ok := v.(json.Number); ok {
			f, err := n.Float64()
			if err != nil {
				return nil, fmt.Errorf("not a number: %s", n)
			}
			return f, nil
		}
	case Bool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case Time:
		if s, ok := v.(string); ok {
			ts, err := time.Parse(timeLayout, s)
			if err != nil {
				return nil, fmt.Errorf("not an RFC 3339 time: %q", s)
			}
			return ts.UTC(), nil
		}
	}
	return nil, fmt.Errorf("wrong JSON type %T for this field", v)
}
