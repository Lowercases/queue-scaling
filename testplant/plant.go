package testplant

type Plant interface {
	Message() chan struct{}
	//SetB() chat float64
}

type Messenger interface {
	Start()
}
