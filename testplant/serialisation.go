package testplant

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

// implements Plant
type Serialiser struct {
	receive  chan struct{}
	filename string
	start    time.Time

	file    *os.File
	encoder *gob.Encoder
}

func NewSerialiser(filename string) *Serialiser {
	return &Serialiser{
		receive:  nil, // Wait to be started
		filename: filename,
	}
}

func (s *Serialiser) Start(initial uint) error {
	var err error

	s.file, err = os.Create(s.filename)
	if err != nil {
		return err
	}
	s.encoder = gob.NewEncoder(s.file)
	s.encoder.Encode(initial)

	s.receive = make(chan struct{})
	s.start = time.Now()
	go s.run()

	return nil
}

func (s *Serialiser) Close() error {
	close(s.receive)
	return s.file.Close()
}

func (s *Serialiser) Message() chan struct{} {
	return s.receive
}

func (s *Serialiser) run() {
	t1 := s.start
	var d time.Duration
	for range s.receive {
		d = time.Since(t1)
		t1 = time.Now()
		s.encoder.Encode(d)
	}
}

// implements Messenger
type Deserialiser struct {
	file     *os.File
	decoder  *gob.Decoder
	receiver chan struct{}
	done     chan bool
	initial  uint
}

func NewDeserialiser(filename string) (*Deserialiser, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	d := &Deserialiser{
		file:    f,
		decoder: gob.NewDecoder(f),
		done:    make(chan bool),
	}
	if err = d.decoder.Decode(&d.initial); err != nil {
		return nil, fmt.Errorf("Cannot decode initial value: %s", err)
	}
	return d, nil
}

func (d *Deserialiser) Start(receiver chan struct{}) {
	d.receiver = receiver
	go d.run()
}

func (d *Deserialiser) run() {
	var val time.Duration
	for d.decoder.Decode(&val) == nil {
		time.Sleep(val)
		d.receiver <- *new(struct{})
	}
	d.file.Close()
	close(d.done)
}

func (d *Deserialiser) Wait() {
	<-d.done
}

func (d *Deserialiser) Initial() uint {
	return d.initial
}
