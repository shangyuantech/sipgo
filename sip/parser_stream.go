package sip

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

const (
	stateStartLine = 0
	stateHeader    = 1
	stateContent   = 2
	// stateParsed = 1
)

var streamBufReader = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}

type ParserStream struct {
	// HeadersParsers uses default list of headers to be parsed. Smaller list parser will be faster
	headersParsers mapHeadersParser

	// runtime values
	reader            *bytes.Buffer
	msg               Message
	readContentLength int
	state             int
}

func (p *ParserStream) reset() {
	p.state = stateStartLine
	p.reader = nil
	p.msg = nil
	p.readContentLength = 0
}

// ParseSIPStream parsing messages comming in stream
// It has slight overhead vs parsing full message
func (p *ParserStream) ParseSIPStream(data []byte) (msgs []Message, err error) {
	if p.reader == nil {
		p.reader = streamBufReader.Get().(*bytes.Buffer)
		p.reader.Reset()
	}

	reader := p.reader
	reader.Write(data)
	unparsed := reader.Bytes()

	parseSingle := func(reader *bytes.Buffer) (msg Message, err error) {
		switch p.state {
		case stateStartLine:
			for {
				startLine, err := nextLine(reader)
				if err != nil {
					if err == io.EOF {
						return nil, ErrParseSipPartial
					}
					return nil, err
				}

				startLine = strings.TrimSpace(startLine)
				if startLine == "" {
					if reader.Len() == 0 {
						return nil, ErrParseSipPartial
					}
					continue
				}

				msg, err = parseLine(startLine)
				if err != nil {
					return nil, err
				}

				p.state = stateHeader
				p.msg = msg
				break
			}
			fallthrough
		case stateHeader:
			msg := p.msg
			for {
				line, err := nextLine(reader)
				if err != nil {
					if err == io.EOF {
						return nil, ErrParseSipPartial
					}
					return nil, err
				}

				if strings.TrimSpace(line) == "" {
					break
				}

				err = p.headersParsers.parseMsgHeader(msg, line)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", err.Error(), ErrParseEOF)
				}
				unparsed = reader.Bytes()
			}
			unparsed = reader.Bytes()

			hdrs := msg.GetHeaders("Content-Length")
			if len(hdrs) == 0 {
				return msg, nil
			}

			h := hdrs[0]
			var contentLength int
			if clh, ok := h.(*ContentLengthHeader); ok {
				contentLength = int(*clh)
			} else {
				n, err := strconv.Atoi(h.Value())
				if err != nil {
					return nil, fmt.Errorf("fail to parse content length: %w", err)
				}
				contentLength = n
			}

			if contentLength <= 0 {
				return msg, nil
			}

			body := make([]byte, contentLength)
			msg.SetBody(body)

			p.state = stateContent
			fallthrough
		case stateContent:
			msg := p.msg
			body := msg.Body()
			contentLength := len(body)

			n, err := reader.Read(body[p.readContentLength:])
			if err != nil {
				if err == io.EOF {
					return nil, ErrParseSipPartial
				}
				return nil, fmt.Errorf("read message body failed: %w", err)
			}
			p.readContentLength += n
			unparsed = reader.Bytes()

			if p.readContentLength < contentLength {
				return nil, ErrParseSipPartial
			}

			p.state = -1
			p.readContentLength = 0
			return msg, nil
		default:
			return nil, fmt.Errorf("Parser is in unknown state")
		}
	}

	for {
		msg, err := parseSingle(reader)
		switch err {
		case ErrParseSipPartial:
			// 数据未完整，保留 buffer，等待下一次读取
			reader.Reset()
			reader.Write(unparsed)
			return msgs, nil
		}

		if err != nil {
			return nil, err
		}

		if msg != nil {
			msgs = append(msgs, msg)
		}

		if len(unparsed) == 0 {
			break
		}

		p.reset()
		reader.Reset()
		reader.Write(unparsed)
		p.reader = reader
	}

	streamBufReader.Put(reader)
	p.reset()
	return msgs, nil
}
