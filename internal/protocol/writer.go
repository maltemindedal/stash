package protocol

import (
	"fmt"
	"io"
	"strconv"
)

// Encode serializes a RESP value into its wire representation.
func Encode(value Value) ([]byte, error) {
	size, err := EncodedLen(value)
	if err != nil {
		return nil, err
	}

	payload, err := appendEncodedValue(make([]byte, 0, size), value)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// EncodeValues serializes multiple RESP values into a single wire payload.
func EncodeValues(values []Value) ([]byte, error) {
	size, err := EncodedValuesLen(values)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, 0, size)
	for _, value := range values {
		payload, err = appendEncodedValue(payload, value)
		if err != nil {
			return nil, err
		}
	}

	return payload, nil
}

// AppendValues appends the wire representations of values to dst in order and
// returns the extended slice. On error dst is returned unmodified in length.
func AppendValues(dst []byte, values []Value) ([]byte, error) {
	original := len(dst)
	for _, value := range values {
		next, err := appendEncodedValue(dst, value)
		if err != nil {
			return dst[:original], err
		}
		dst = next
	}
	return dst, nil
}

// EncodedLen reports how many bytes Encode would emit for value.
func EncodedLen(value Value) (int, error) {
	switch typed := value.(type) {
	case SimpleString:
		return 1 + len(typed.Value) + 2, nil
	case ErrorValue:
		return 1 + len(typed.Message) + 2, nil
	case Integer:
		return 1 + decimalIntLen(typed.Value) + 2, nil
	case BulkString:
		if typed.Null {
			return len("$-1\r\n"), nil
		}
		return 1 + decimalIntLen(int64(len(typed.Data))) + 2 + len(typed.Data) + 2, nil
	case TextBulkString:
		if typed.Null {
			return len("$-1\r\n"), nil
		}
		return 1 + decimalIntLen(int64(len(typed.Value))) + 2 + len(typed.Value) + 2, nil
	case Array:
		if typed.Null {
			return len("*-1\r\n"), nil
		}

		size := 1 + decimalIntLen(int64(len(typed.Elements))) + 2
		for _, element := range typed.Elements {
			elementLen, err := EncodedLen(element)
			if err != nil {
				return 0, err
			}
			size += elementLen
		}
		return size, nil
	case Boolean:
		return len("#t\r\n"), nil
	case Null:
		return len("_\r\n"), nil
	default:
		return 0, fmt.Errorf("protocol: unsupported value type %T", value)
	}
}

// EncodedValuesLen reports how many bytes EncodeValues would emit for values.
func EncodedValuesLen(values []Value) (int, error) {
	total := 0
	for _, value := range values {
		size, err := EncodedLen(value)
		if err != nil {
			return 0, err
		}
		total += size
	}

	return total, nil
}

// WriteValue writes a RESP value to the provided writer.
func WriteValue(writer io.Writer, value Value) error {
	switch typed := value.(type) {
	case SimpleString:
		return writePrefixedStringLine(writer, '+', typed.Value)
	case ErrorValue:
		return writePrefixedStringLine(writer, '-', typed.Message)
	case Integer:
		return writePrefixedIntLine(writer, ':', typed.Value)
	case BulkString:
		if typed.Null {
			_, err := io.WriteString(writer, "$-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '$', int64(len(typed.Data))); err != nil {
			return err
		}
		if _, err := writer.Write(typed.Data); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	case TextBulkString:
		if typed.Null {
			_, err := io.WriteString(writer, "$-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '$', int64(len(typed.Value))); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, typed.Value); err != nil {
			return err
		}
		_, err := io.WriteString(writer, "\r\n")
		return err
	case Array:
		if typed.Null {
			_, err := io.WriteString(writer, "*-1\r\n")
			return err
		}

		if err := writePrefixedIntLine(writer, '*', int64(len(typed.Elements))); err != nil {
			return err
		}
		for _, element := range typed.Elements {
			if err := WriteValue(writer, element); err != nil {
				return err
			}
		}
		return nil
	case Boolean:
		if typed.Value {
			_, err := io.WriteString(writer, "#t\r\n")
			return err
		}
		_, err := io.WriteString(writer, "#f\r\n")
		return err
	case Null:
		_, err := io.WriteString(writer, "_\r\n")
		return err
	default:
		return fmt.Errorf("protocol: unsupported value type %T", value)
	}
}

func appendEncodedValue(dst []byte, value Value) ([]byte, error) {
	switch typed := value.(type) {
	case SimpleString:
		return appendPrefixedStringLine(dst, '+', typed.Value), nil
	case ErrorValue:
		return appendPrefixedStringLine(dst, '-', typed.Message), nil
	case Integer:
		return appendPrefixedIntLine(dst, ':', typed.Value), nil
	case BulkString:
		if typed.Null {
			return append(dst, "$-1\r\n"...), nil
		}

		dst = appendPrefixedIntLine(dst, '$', int64(len(typed.Data)))
		dst = append(dst, typed.Data...)
		return append(dst, '\r', '\n'), nil
	case TextBulkString:
		if typed.Null {
			return append(dst, "$-1\r\n"...), nil
		}

		dst = appendPrefixedIntLine(dst, '$', int64(len(typed.Value)))
		dst = append(dst, typed.Value...)
		return append(dst, '\r', '\n'), nil
	case Array:
		if typed.Null {
			return append(dst, "*-1\r\n"...), nil
		}

		dst = appendPrefixedIntLine(dst, '*', int64(len(typed.Elements)))
		var err error
		for _, element := range typed.Elements {
			dst, err = appendEncodedValue(dst, element)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case Boolean:
		if typed.Value {
			return append(dst, "#t\r\n"...), nil
		}
		return append(dst, "#f\r\n"...), nil
	case Null:
		return append(dst, "_\r\n"...), nil
	default:
		return nil, fmt.Errorf("protocol: unsupported value type %T", value)
	}
}

func writePrefixedStringLine(writer io.Writer, prefix byte, value string) error {
	if err := writeByte(writer, prefix); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, value); err != nil {
		return err
	}
	_, err := io.WriteString(writer, "\r\n")
	return err
}

func writePrefixedIntLine(writer io.Writer, prefix byte, value int64) error {
	if err := writeByte(writer, prefix); err != nil {
		return err
	}
	var buf [32]byte
	encoded := strconv.AppendInt(buf[:0], value, 10)
	if _, err := writer.Write(encoded); err != nil {
		return err
	}
	_, err := io.WriteString(writer, "\r\n")
	return err
}

func appendPrefixedStringLine(dst []byte, prefix byte, value string) []byte {
	dst = append(dst, prefix)
	dst = append(dst, value...)
	return append(dst, '\r', '\n')
}

func appendPrefixedIntLine(dst []byte, prefix byte, value int64) []byte {
	dst = append(dst, prefix)
	dst = strconv.AppendInt(dst, value, 10)
	return append(dst, '\r', '\n')
}

func decimalIntLen(value int64) int {
	var buf [32]byte
	return len(strconv.AppendInt(buf[:0], value, 10))
}

func writeByte(writer io.Writer, value byte) error {
	if byteWriter, ok := writer.(io.ByteWriter); ok {
		return byteWriter.WriteByte(value)
	}

	_, err := writer.Write([]byte{value})
	return err
}
